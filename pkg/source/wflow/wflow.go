// Package wflow ingests from the wflow.com API (wflow is the SaaS that fronts
// Pohoda accounting; this is NOT Pohoda's own mServer/XML API).
package wflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/ingestr/internal/config"
	"github.com/bruin-data/ingestr/pkg/arrowconv"
	httpclient "github.com/bruin-data/ingestr/pkg/http"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
	"resty.dev/v3"
)

const (
	apiBaseURL    = "https://api.wflow.com"
	oauthTokenURL = "https://account.wflow.com/connect/token"
	// The published spec advertises /connect/authorize as the tokenUrl. That is
	// wrong; the OIDC discovery document gives /connect/token.
	oauthScope = "uccl_common_api"

	accessTokenSkew = 5 * time.Minute

	// The API silently caps pageSize at 100: request more and it returns 100
	// and echoes pageSize:100. Requesting anything larger is a lie to yourself.
	maxPageSize = 100
	// Guard against a totalItems that never satisfies the loop.
	maxPages = 10000

	// x-rate-limit-remaining: 189 over x-rate-limit-period 00:01:00, so ~190/min.
	// 80% of that per the source guidelines: (190*0.8)/60 = 2.53 req/s.
	rateLimit      = 2.53
	rateLimitBurst = 5
)

// registerTables are the /registers/<name> collections. They carry NO
// timestamp field of any kind (verified against the live API), so none of them
// can be loaded incrementally -- they are replace-only reference data.
var registerTables = []string{
	"accountingrules", "activities", "businesscases", "businessitemcategories",
	"businessitems", "carddocumenttypes", "cashdocumenttypes", "cashregisters",
	"chartofaccounts", "contracts", "costcenters", "employees", "locations",
	"measureunits", "organizationpersons", "partnerpersons", "partners",
	"paymentmethods", "projects", "series", "vatcontrolstatementlines",
	"vatreturnlines", "vatreversechargecodes", "vehicles",
}

// tablePrimaryKeys overrides the default `id` primary key.
//
// 🔴 Not every sub-resource has an id. `approvals` returns approval STEPS
// ({level, team, identity, date, status}) with no identifier of their own --
// they are unique per (document, level). Assuming `id` everywhere fails at
// destination-table creation with `does not have a column named "id"`.
var tablePrimaryKeys = map[string][]string{
	"document_approvals": {"documentId", "level"},
}

// primaryKeysFor returns the merge key, ALWAYS prefixed with the organization.
// The tables are shared by every company, so `id` alone is only unique by
// accident of wflow using UUIDs -- an org-scoped sequence anywhere would make
// one company's row replace another's, silently and irreversibly.
func primaryKeysFor(table string) []string {
	pk, ok := tablePrimaryKeys[table]
	if !ok {
		pk = []string{"id"}
	}
	return append([]string{orgColumn}, pk...)
}

// documentSubTables fan out per document id.
var documentSubTables = map[string]string{
	"document_events":    "events",
	"document_files":     "files",
	"document_approvals": "approvals",
	"document_comments":  "comments",
	"document_links":     "links",
	"document_tags":      "tags",
}

type WflowSource struct {
	client       *httpclient.Client
	clientID     string
	clientSecret string
	// organization is DISCOVERED from the credential, never configured: each
	// client is scoped to exactly one org and /api/user/myorganizations names
	// it. Configuring it invites a key labelled with another company's slug,
	// which would silently ingest the wrong company under the wrong name.
	organization string

	accessToken string
	tokenExpiry time.Time
}

func NewWflowSource() *WflowSource { return &WflowSource{} }

func (s *WflowSource) Schemes() []string { return []string{"wflow"} }

func (s *WflowSource) HandlesIncrementality() bool { return true }

func (s *WflowSource) Connect(ctx context.Context, uri string) error {
	id, secret, org, err := parseWflowURI(uri)
	if err != nil {
		return err
	}
	s.clientID, s.clientSecret, s.organization = id, secret, org

	if err := s.ensureAccessToken(ctx); err != nil {
		return fmt.Errorf("failed to obtain access token: %w", err)
	}
	if s.organization == "" {
		if err := s.discoverOrganization(ctx); err != nil {
			return err
		}
	}
	config.Debug("[WFLOW] connected, organization=%s", s.organization)
	return nil
}

func parseWflowURI(uri string) (clientID, clientSecret, organization string, err error) {
	if !strings.HasPrefix(uri, "wflow://") {
		return "", "", "", fmt.Errorf("invalid wflow URI: must start with wflow://")
	}
	rest := strings.TrimPrefix(strings.TrimPrefix(uri, "wflow://"), "?")
	values, perr := url.ParseQuery(rest)
	if perr != nil {
		return "", "", "", fmt.Errorf("failed to parse wflow URI query: %w", perr)
	}
	clientID = values.Get("client_id")
	clientSecret = values.Get("client_secret")
	if clientID == "" || clientSecret == "" {
		return "", "", "", fmt.Errorf("client_id and client_secret are required in wflow URI")
	}
	// Optional. Omit it and the org is discovered from the credential, which is
	// what you want; supply it only to pin a specific org for a multi-org key.
	organization = values.Get("organization")
	return clientID, clientSecret, organization, nil
}

func (s *WflowSource) refreshAccessToken(ctx context.Context) error {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.clientID)
	form.Set("client_secret", s.clientSecret)
	form.Set("scope", oauthScope)

	c := resty.New().SetTimeout(60 * time.Second)
	defer func() { _ = c.Close() }()

	resp, err := c.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetBody(form.Encode()).
		Post(oauthTokenURL)
	if err != nil {
		return fmt.Errorf("oauth token request failed: %w", err)
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return fmt.Errorf("oauth token request returned status %d: %s", resp.StatusCode(), resp.String())
	}

	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(resp.Bytes(), &body); err != nil {
		return fmt.Errorf("failed to parse oauth token response: %w", err)
	}
	if body.AccessToken == "" {
		return fmt.Errorf("oauth token response missing access_token")
	}

	s.accessToken = body.AccessToken
	ttl := time.Duration(body.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	s.tokenExpiry = time.Now().Add(ttl)
	config.Debug("[WFLOW] access_token refreshed, expires in %s", ttl)
	return nil
}

// ensureAccessToken refreshes ahead of expiry. Tokens last 3600s and a full
// crawl can outlive that, so this is called before every request batch.
func (s *WflowSource) ensureAccessToken(ctx context.Context) error {
	if s.accessToken != "" && time.Until(s.tokenExpiry) > accessTokenSkew {
		return nil
	}
	if err := s.refreshAccessToken(ctx); err != nil {
		return err
	}
	if s.client != nil {
		_ = s.client.Close()
	}
	s.client = httpclient.New(
		httpclient.WithBaseURL(apiBaseURL),
		httpclient.WithTimeout(120*time.Second),
		httpclient.WithRateLimiter(rateLimit, rateLimitBurst),
		httpclient.WithAuth(httpclient.NewBearerAuth(s.accessToken)),
		httpclient.WithDebug(config.DebugMode),
		httpclient.WithHeader("Accept", "application/json"),
	)
	return nil
}

func (s *WflowSource) discoverOrganization(ctx context.Context) error {
	// 🔴 Via getJSON: a 5xx here previously fell through to "credential has access
	// to no organizations", i.e. a transient outage was reported as a DEAD KEY.
	orgBody, err := s.getJSON(ctx, "/api/user/myorganizations")
	if err != nil {
		return fmt.Errorf("failed to list organizations: %w", err)
	}
	var orgs []struct {
		Name      string `json:"name"`
		Subdomain string `json:"subdomain"`
	}
	if err := json.Unmarshal(orgBody, &orgs); err != nil {
		return fmt.Errorf("failed to parse organizations: %w", err)
	}
	if len(orgs) == 0 {
		return fmt.Errorf("credential has access to no organizations")
	}
	if len(orgs) > 1 {
		names := make([]string, 0, len(orgs))
		for _, o := range orgs {
			names = append(names, o.Subdomain)
		}
		return fmt.Errorf("credential has access to %d organizations (%s); pass organization= in the URI to choose one",
			len(orgs), strings.Join(names, ", "))
	}
	s.organization = orgs[0].Subdomain
	return nil
}

func (s *WflowSource) Close(ctx context.Context) error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func isValidTable(name string) bool {
	if name == "documents" {
		return true
	}
	if _, ok := documentSubTables[name]; ok {
		return true
	}
	for _, r := range registerTables {
		if name == "registers_"+r {
			return true
		}
	}
	return false
}

func (s *WflowSource) GetTable(ctx context.Context, req source.TableRequest) (source.SourceTable, error) {
	name := req.Name
	if !isValidTable(name) {
		return nil, fmt.Errorf("unsupported table: %s", name)
	}

	// Only `documents` can be loaded incrementally: it is the one collection
	// whose rows carry `updated`. Registers have no timestamp at all, and the
	// document sub-resources are reached through their parent, so both are
	// full-refresh.
	strategy := config.StrategyReplace
	incrementalKey := ""
	if name == "documents" {
		strategy = config.StrategyMerge
		incrementalKey = "updated"
	} else if _, ok := documentSubTables[name]; ok {
		strategy = config.StrategyMerge
	}

	return &source.DynamicSourceTable{
		TableName:           name,
		TablePrimaryKeys:    primaryKeysFor(name),
		TableIncrementalKey: incrementalKey,
		TableStrategy:       strategy,
		KnownSchema:         false,
		SchemaFn: func(ctx context.Context) (*schema.TableSchema, error) {
			return nil, fmt.Errorf("wflow source infers its schema from the data")
		},
		ReadFn: func(ctx context.Context, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
			return s.read(ctx, name, opts)
		},
	}, nil
}

func (s *WflowSource) read(ctx context.Context, table string, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
	results := make(chan source.RecordBatchResult, 8)

	go func() {
		defer close(results)
		var err error
		switch {
		case table == "documents":
			err = s.readDocuments(ctx, opts, results)
		case strings.HasPrefix(table, "registers_"):
			err = s.readRegister(ctx, strings.TrimPrefix(table, "registers_"), opts, results)
		default:
			sub, ok := documentSubTables[table]
			if !ok {
				err = fmt.Errorf("unsupported table: %s", table)
			} else {
				err = s.readDocumentSub(ctx, table, sub, opts, results)
			}
		}
		if err != nil {
			results <- source.RecordBatchResult{Err: err}
		}
	}()

	return results, nil
}

// buildQuery renders the wflow filter grammar: `{property} {operator} {value}`
// joined by ` and `. Operators are SYMBOLIC -- `>` `>=` `=`. The OData spellings
// (gt/ge/eq) return HTTP 500 "The given key 'gt' was not present in the
// dictionary", and the grammar appears only in a 400 body, not in the docs.
func buildIncrementalQuery(field string, since time.Time) string {
	return fmt.Sprintf("%s > %s", field, since.UTC().Format("2006-01-02T15:04:05"))
}

func (s *WflowSource) readDocuments(ctx context.Context, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	config.Debug("[WFLOW] reading documents")
	q := url.Values{}
	if opts.IntervalStart != nil {
		q.Set("query", buildIncrementalQuery("updated", *opts.IntervalStart))
	}
	return s.paginateAndSend(ctx, s.orgPath("/documents"), q, opts, results)
}

func (s *WflowSource) readRegister(ctx context.Context, register string, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	config.Debug("[WFLOW] reading register %s", register)
	// No incremental filter: these carry no timestamp, so every run is a full
	// page-through. They are small (hundreds of rows) by nature.
	return s.paginateAndSend(ctx, s.orgPath("/registers/"+register), url.Values{}, opts, results)
}

// readDocumentSub fans out over document ids. The sub-resources have no
// collection endpoint of their own, so the parent list is walked first; when the
// run is incremental that parent list is already filtered, which bounds the
// fan-out to documents that actually changed.
func (s *WflowSource) readDocumentSub(ctx context.Context, table, sub string, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	config.Debug("[WFLOW] reading %s", table)

	ids, err := s.documentIDs(ctx, opts)
	if err != nil {
		return err
	}
	config.Debug("[WFLOW] %s: fanning out over %d documents", table, len(ids))

	progress("%s: fanning out over %d documents", table, len(ids))
	for i, id := range ids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if i > 0 && i%progressEvery == 0 {
			progress("%s: %d/%d documents", table, i, len(ids))
		}
		body, err := s.getJSON(ctx, s.orgPath("/documents/"+id+"/"+sub))
		if err != nil {
			return fmt.Errorf("failed to read %s for document %s: %w", sub, id, err)
		}
		items, err := decodeItems(body)
		if err != nil {
			return fmt.Errorf("failed to parse %s for document %s: %w", sub, id, err)
		}
		if len(items) == 0 {
			continue
		}
		// The sub-resource rows do not all carry the parent id, so add it --
		// without it the rows cannot be joined back to their document.
		for _, it := range items {
			it["documentId"] = id
		}
		if err := s.sendBatch(items, opts, results); err != nil {
			return err
		}
	}
	return nil
}

func (s *WflowSource) documentIDs(ctx context.Context, opts source.ReadOptions) ([]string, error) {
	q := url.Values{}
	if opts.IntervalStart != nil {
		q.Set("query", buildIncrementalQuery("updated", *opts.IntervalStart))
	}

	ids := make([]string, 0, 128)
	page := 0 // zero-indexed, see paginateAndSend
	for page < maxPages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if err := s.ensureAccessToken(ctx); err != nil {
			return nil, err
		}
		items, total, err := s.fetchPage(ctx, s.orgPath("/documents"), q, page)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if id, ok := it["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
		if len(items) == 0 || len(ids) >= total {
			break
		}
		page++
	}
	return ids, nil
}

func (s *WflowSource) orgPath(suffix string) string {
	return "/api/" + s.organization + suffix
}

// fetchPage returns one page of a collection plus the reported total.
func (s *WflowSource) fetchPage(ctx context.Context, path string, q url.Values, page int) ([]map[string]interface{}, int, error) {
	params := url.Values{}
	for k, v := range q {
		params[k] = v
	}
	params.Set("page", fmt.Sprintf("%d", page))
	params.Set("pageSize", fmt.Sprintf("%d", maxPageSize))

	body, err := s.getJSON(ctx, path+"?"+params.Encode())
	if err != nil {
		return nil, 0, fmt.Errorf("request to %s failed: %w", path, err)
	}

	var env struct {
		Items      []map[string]interface{} `json:"items"`
		TotalItems int                      `json:"totalItems"`
		PageSize   int                      `json:"pageSize"`
		Page       int                      `json:"page"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, 0, fmt.Errorf("failed to parse response from %s: %w", path, err)
	}
	return env.Items, env.TotalItems, nil
}

// paginateAndSend walks a collection endpoint, streaming one batch per page.
func (s *WflowSource) paginateAndSend(ctx context.Context, path string, q url.Values, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	seen := 0
	lastReported := 0
	// 🔴 Pages are ZERO-indexed. Starting at 1 silently skips the first page,
	// and for any collection that fits in one page it returns NOTHING while
	// still reporting HTTP 200 and a correct totalItems.
	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := s.ensureAccessToken(ctx); err != nil {
			return err
		}

		items, total, err := s.fetchPage(ctx, path, q, page)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		if err := s.sendBatch(items, opts, results); err != nil {
			return err
		}

		seen += len(items)
		if seen-lastReported >= progressEvery {
			progress("%s: %d/%d rows", path, seen, total)
			lastReported = seen
		}
		if seen >= total {
			return nil
		}
		if page == maxPages-1 {
			config.Debug("[WFLOW] maxPages (%d) reached for %s; %d of %d rows read", maxPages, path, seen, total)
		}
	}
	return nil
}

// decodeItems handles both response shapes: the sub-resources return a flat
// array, except approvals which wraps them in {pathName, items}.
func decodeItems(body []byte) ([]map[string]interface{}, error) {
	var arr []map[string]interface{}
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}
	// 🔴 A POINTER, so "items": [] (a real empty collection) is distinguishable
	// from an object with no `items` key at all. Without that distinction an API
	// error body like {"message":"Too Many Requests"} unmarshals cleanly, yields
	// a nil slice, and the caller treats it as "this document has no comments" —
	// silent data loss. getJSON now rejects non-2xx before we get here; this is
	// the second line of defence, for a 200 that is not a collection.
	var env struct {
		Items *[]map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if env.Items == nil {
		return nil, fmt.Errorf("response is neither an array nor an {items:[…]} envelope: %s", truncate(string(body), 200))
	}
	return *env.Items, nil
}

// progress writes a heartbeat to STDERR, unconditionally.
//
// 🔴 Deliberately NOT config.Debug: that is a no-op unless --debug is passed, and
// turning --debug on just to see a progress line buries it in HTTP
// tracing. The sub-resource fan-out is one serial request per document under a
// ~2.5 req/s rate limit, so a large org spends over an hour inside a SINGLE
// ingestr invocation, printing nothing. Silence for that long is indistinguishable
// from a hang, and someone eventually kills a run that was working.
//
// Rate-limited to one line per progressEvery documents so it stays readable.
const progressEvery = 50

// Retries for a 429 or 5xx on a data request.
const maxRetries = 4

func progress(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[WFLOW] "+format+"\n", args...)
}

// getJSON is the ONLY way the data path should issue a request.
//
// 🔴 The client does NOT treat a non-2xx response as an error — `err` is nil and
// the body is the API's error JSON. Combined with decodeItems (which happily
// unmarshals `{"message":"Too Many Requests"}` into a struct with a nil Items
// slice) that produced SILENT DATA LOSS: a 429/500/403 on one document returned
// zero items, the caller hit `len(items) == 0 { continue }`, and the document was
// skipped with no error, no log line, and a zero exit code. Proven by test.
//
// 429 is retried with backoff — we self-limit to 80% of the documented quota, so
// a 429 means the estimate is off, not that the data is gone.
func (s *WflowSource) getJSON(ctx context.Context, path string) ([]byte, error) {
	var lastStatus int
	var lastBody string
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := s.ensureAccessToken(ctx); err != nil {
			return nil, err
		}
		resp, err := s.client.R(ctx).Get(path)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", path, err)
		}
		code := resp.StatusCode()
		if code >= 200 && code < 300 {
			return resp.Body(), nil
		}
		lastStatus, lastBody = code, truncate(resp.String(), 200)

		retryable := code == http.StatusTooManyRequests || code >= 500
		if !retryable || attempt == maxRetries {
			break
		}
		wait := time.Duration(1<<attempt) * time.Second
		if ra := resp.Header().Get("Retry-After"); ra != "" {
			if secs, convErr := strconv.Atoi(ra); convErr == nil && secs > 0 {
				wait = time.Duration(secs) * time.Second
			}
		}
		progress("GET %s -> %d, retrying in %s (attempt %d/%d)", path, code, wait, attempt+1, maxRetries)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("GET %s returned status %d: %s", path, lastStatus, lastBody)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// orgColumn is stamped onto every row of every table.
//
// 🔴 Without it the warehouse is unusable for a multi-company tenant. One
// credential == one company, and the sync loop points all 13 credentials at the
// SAME destination tables, so `raw_pohoda.documents` is the pooled invoices of
// every company with nothing to tell them apart -- no filtering, no per-company
// access control, no correct totals. wflow's ids happen to be UUIDs, so the rows
// do not currently collide; that is luck, not a design, and it does nothing for
// attribution. The column is also part of the merge key (see primaryKeysFor) so
// a future non-UUID id cannot silently overwrite another company's row.
const orgColumn = "organization"

func (s *WflowSource) sendBatch(items []map[string]interface{}, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	for _, it := range items {
		it[orgColumn] = s.organization
	}
	rec, err := arrowconv.ItemsToArrowRecordWithSchema(items, nil, opts.ExcludeColumns)
	if err != nil {
		return fmt.Errorf("failed to build record batch: %w", err)
	}
	results <- source.RecordBatchResult{Batch: rec}
	return nil
}
