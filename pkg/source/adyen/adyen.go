// Package adyen implements an ingestr source for Adyen's Balance Platform.
//
// Adyen is not one API. This source spans two of them, on the same host but
// different path prefixes and with DIFFERENT PAGINATION STYLES:
//
//	/btl/v4  Balance Platform Transfers — transfers, transactions. Cursor paging.
//	/bcl/v2  Balance Platform Configuration — account holders, balance accounts,
//	         payment instruments. Offset paging (hasNext/hasPrevious).
//
// Test and live are different HOSTNAMES, not a path, and an API key is scoped per
// API and per account — a key that opens Balance Platform will 403 on Legal Entity
// Management and vice versa. Legal Entity Management is deliberately not covered
// here: it needs a second key and its payloads carry personal data that transfers
// do not.
package adyen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bruin-data/ingestr/internal/config"
	"github.com/bruin-data/ingestr/pkg/arrowconv"
	httpclient "github.com/bruin-data/ingestr/pkg/http"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
)

const (
	liveHost = "https://balanceplatform-api-live.adyen.com"
	testHost = "https://balanceplatform-api-test.adyen.com"

	transfersPrefix = "/btl/v4"
	configPrefix    = "/bcl/v2"

	// maxPageSize is the API's documented ceiling on `limit`; the default is 10,
	// so not setting it makes a backfill 10x more requests than it needs.
	maxPageSize = 100

	// maxWindow is a HARD API CONSTRAINT, not a tuning choice. /transfers and
	// /transactions reject a createdSince..createdUntil span wider than 6 months,
	// so any interval is walked in windows no larger than this.
	maxWindow = 180 * 24 * time.Hour

	// maxPages bounds each pagination loop. Cursor paging has no total count, so
	// this is a runaway guard against a cursor that never terminates.
	maxPages = 100000

	// rateLimit is SELF-IMPOSED. Adyen does not publish a per-endpoint request
	// ceiling for the Balance Platform APIs, so this is deliberately conservative
	// rather than tuned to a documented number. Override with ?rate_limit= when
	// backfilling.
	defaultRateLimit = 8.0
	rateLimitBurst   = 5

	httpTimeout = 60 * time.Second

	// historyFloor is used when no --interval-start is given. The endpoints require
	// an explicit lower bound, so "no interval" has to mean SOME date; picking a
	// floor well before the platform existed makes it mean "everything" rather than
	// silently defaulting to a recent window.
	historyFloor = "2020-01-01T00:00:00Z"
)

// tableConfig describes one exposed table.
type tableConfig struct {
	// api selects the path prefix and, with it, the pagination style.
	api string // "transfers" (btl, cursor) or "config" (bcl, offset)
	// path is the endpoint, with {accountHolderId}/{balanceAccountId} placeholders
	// for tables that fan out over a parent.
	path string
	// listKey is the response field holding the array.
	listKey string
	// windowed marks endpoints that REQUIRE createdSince and createdUntil.
	windowed bool
	// fanOut names the parent table this one iterates over, if any.
	fanOut string
	// primaryKeys are lifted to top-level columns; everything else stays nested.
	primaryKeys []string
	// incrementalKey is set only where the API can filter server-side.
	incrementalKey string
}

var supportedTables = map[string]tableConfig{
	// Transfers carry the money movement. Refunds and payouts are NOT separate
	// resources — both are transfers distinguished by `category` (payout = bank,
	// refund = platformPayment), so splitting them into their own tables would
	// duplicate every row.
	"transfers": {
		api: "transfers", path: "/transfers", listKey: "data", windowed: true,
		primaryKeys: []string{"id"}, incrementalKey: "createdAt",
	},
	// Transactions are the balance-account ledger view of the same movements.
	// This endpoint rejects `category` and `reference` outright (422); it takes
	// only the scoping id, the created window and paging.
	"transactions": {
		api: "transfers", path: "/transactions", listKey: "data", windowed: true,
		primaryKeys: []string{"id"}, incrementalKey: "creationDate",
	},
	// The configuration tables exist to make the transfer rows joinable — a
	// transfer carries accountHolder.id and balanceAccount.id but no names or
	// status beyond them.
	"account_holders": {
		api: "config", path: "/balancePlatforms/{balancePlatform}/accountHolders",
		listKey: "accountHolders", primaryKeys: []string{"id"},
	},
	"balance_accounts": {
		api: "config", path: "/accountHolders/{accountHolderId}/balanceAccounts",
		listKey: "balanceAccounts", fanOut: "account_holders", primaryKeys: []string{"id"},
	},
	"payment_instruments": {
		api: "config", path: "/balanceAccounts/{balanceAccountId}/paymentInstruments",
		listKey: "paymentInstruments", fanOut: "balance_accounts", primaryKeys: []string{"id"},
	},
}

func supportedTableNames() string {
	names := make([]string, 0, len(supportedTables))
	for n := range supportedTables {
		names = append(names, n)
	}
	return strings.Join(names, ", ")
}

type Source struct {
	apiKey          string
	balancePlatform string
	host            string
	rateLimit       float64
	client          *httpclient.Client
}

func NewAdyenSource() *Source { return &Source{} }

func (s *Source) Schemes() []string { return []string{"adyen"} }

// HandlesIncrementality is true: the windowed endpoints filter server-side, and
// the config tables are snapshots the source fetches in full.
func (s *Source) HandlesIncrementality() bool { return true }

type adyenConfig struct {
	apiKey          string
	balancePlatform string
	environment     string
	rateLimit       float64
}

func parseURI(uri string) (adyenConfig, error) {
	var cfg adyenConfig

	u, err := url.Parse(uri)
	if err != nil {
		return cfg, fmt.Errorf("invalid adyen URI: %w", err)
	}
	if u.Scheme != "adyen" {
		return cfg, fmt.Errorf("invalid scheme %q, expected adyen://", u.Scheme)
	}

	q := u.Query()
	cfg.apiKey = q.Get("api_key")
	if cfg.apiKey == "" {
		return cfg, fmt.Errorf("api_key is required: adyen://?api_key=<key>&balance_platform=<id>")
	}

	// Required and never defaulted. /transfers accepts exactly ONE of
	// balancePlatform / balanceAccountId / accountHolderId — sending two is a 422 —
	// and without any of them there is no request to make.
	cfg.balancePlatform = q.Get("balance_platform")
	if cfg.balancePlatform == "" {
		return cfg, fmt.Errorf("balance_platform is required: adyen://?api_key=<key>&balance_platform=<id>")
	}

	cfg.environment = q.Get("environment")
	if cfg.environment == "" {
		cfg.environment = "live"
	}
	if cfg.environment != "live" && cfg.environment != "test" {
		return cfg, fmt.Errorf("environment must be live or test, got %q", cfg.environment)
	}

	cfg.rateLimit = defaultRateLimit
	if v := q.Get("rate_limit"); v != "" {
		var rl float64
		if _, err := fmt.Sscanf(v, "%f", &rl); err != nil || rl <= 0 {
			return cfg, fmt.Errorf("rate_limit must be a positive number, got %q", v)
		}
		cfg.rateLimit = rl
	}

	return cfg, nil
}

func (s *Source) Connect(ctx context.Context, uri string) error {
	cfg, err := parseURI(uri)
	if err != nil {
		return err
	}
	s.apiKey = cfg.apiKey
	s.balancePlatform = cfg.balancePlatform
	s.rateLimit = cfg.rateLimit
	s.host = liveHost
	if cfg.environment == "test" {
		s.host = testHost
	}

	s.client = httpclient.New(
		httpclient.WithBaseURL(s.host),
		httpclient.WithTimeout(httpTimeout),
		httpclient.WithRateLimiter(cfg.rateLimit, rateLimitBurst),
		httpclient.WithAuth(httpclient.NewAPIKeyAuth("X-API-Key", cfg.apiKey, true)),
		httpclient.WithDebug(config.DebugMode),
		httpclient.WithHeader("Accept", "application/json"),
	)
	return nil
}

func (s *Source) Close(ctx context.Context) error { return nil }

func isValidTable(name string) bool {
	_, ok := supportedTables[name]
	return ok
}

func (s *Source) GetTable(ctx context.Context, req source.TableRequest) (source.SourceTable, error) {
	tc, ok := supportedTables[req.Name]
	if !ok {
		return nil, fmt.Errorf("unsupported adyen table %q; supported tables: %s", req.Name, supportedTableNames())
	}

	strategy := config.StrategyMerge
	if req.StrategySet {
		strategy = req.Strategy
	}

	pks := tc.primaryKeys
	if len(req.PrimaryKeys) > 0 {
		pks = req.PrimaryKeys
	}

	return &source.DynamicSourceTable{
		TableName:           req.Name,
		TablePrimaryKeys:    pks,
		TableIncrementalKey: tc.incrementalKey,
		TableStrategy:       strategy,
		KnownSchema:         false,
		SchemaFn: func(ctx context.Context) (*schema.TableSchema, error) {
			return nil, fmt.Errorf("adyen source does not have a predefined schema; schema inference is required")
		},
		ReadFn: func(ctx context.Context, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
			return s.read(ctx, req.Name, opts)
		},
	}, nil
}

func (s *Source) read(ctx context.Context, table string, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
	if !isValidTable(table) {
		return nil, fmt.Errorf("unsupported adyen table %q; supported tables: %s", table, supportedTableNames())
	}
	results := make(chan source.RecordBatchResult, 8)
	go func() {
		defer close(results)
		if err := s.readTable(ctx, table, opts, results); err != nil {
			results <- source.RecordBatchResult{Err: err}
		}
	}()
	return results, nil
}

func (s *Source) readTable(ctx context.Context, table string, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	tc := supportedTables[table]
	config.Debug("[ADYEN] reading %s", table)

	switch {
	case tc.windowed:
		return s.readWindowed(ctx, table, tc, opts, results)
	case tc.fanOut != "":
		return s.readFanOut(ctx, table, tc, opts, results)
	default:
		path := strings.ReplaceAll(tc.path, "{balancePlatform}", s.balancePlatform)
		return s.readOffsetPaged(ctx, table, tc, path, nil, opts, results)
	}
}

// readWindowed walks the interval in windows no wider than maxWindow.
//
// Both createdSince and createdUntil are REQUIRED by these endpoints, so there is
// no "just fetch everything" request — a backfill is chunked by construction.
func (s *Source) readWindowed(ctx context.Context, table string, tc tableConfig, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	start, err := time.Parse(time.RFC3339, historyFloor)
	if err != nil {
		return err
	}
	if opts.IntervalStart != nil {
		start = *opts.IntervalStart
	}
	end := time.Now().UTC()
	if opts.IntervalEnd != nil {
		end = *opts.IntervalEnd
	}
	if !end.After(start) {
		return fmt.Errorf("adyen %s: interval end %s is not after start %s", table, end.Format(time.RFC3339), start.Format(time.RFC3339))
	}

	for _, w := range windows(start, end) {
		winStart, winEnd := w[0], w[1]
		params := map[string]string{
			"balancePlatform": s.balancePlatform,
			"createdSince":    winStart.UTC().Format(time.RFC3339),
			"createdUntil":    winEnd.UTC().Format(time.RFC3339),
		}
		config.Debug("[ADYEN] %s window %s .. %s", table, params["createdSince"], params["createdUntil"])
		if err := s.readCursorPaged(ctx, table, tc, tc.path, params, opts, results); err != nil {
			return err
		}
	}
	return nil
}

// windows splits [start, end) into spans no wider than maxWindow, which the
// transfer endpoints enforce: a wider createdSince..createdUntil is rejected.
func windows(start, end time.Time) [][2]time.Time {
	var out [][2]time.Time
	for s := start; s.Before(end); {
		e := s.Add(maxWindow)
		if e.After(end) {
			e = end
		}
		out = append(out, [2]time.Time{s, e})
		s = e
	}
	return out
}

// readFanOut iterates a parent table's ids and reads the child endpoint for each.
func (s *Source) readFanOut(ctx context.Context, table string, tc tableConfig, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	parentIDs, err := s.collectIDs(ctx, tc.fanOut)
	if err != nil {
		return fmt.Errorf("adyen %s: listing parent %s: %w", table, tc.fanOut, err)
	}
	config.Debug("[ADYEN] %s fanning out over %d %s", table, len(parentIDs), tc.fanOut)

	placeholder := "{accountHolderId}"
	if tc.fanOut == "balance_accounts" {
		placeholder = "{balanceAccountId}"
	}
	for _, id := range parentIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		path := strings.ReplaceAll(tc.path, placeholder, id)
		if err := s.readOffsetPaged(ctx, table, tc, path, nil, opts, results); err != nil {
			return fmt.Errorf("adyen %s for parent %s: %w", table, id, err)
		}
	}
	return nil
}

// collectIDs lists a parent table and returns its ids. Used only to drive fan-out,
// so it buffers ids rather than rows.
func (s *Source) collectIDs(ctx context.Context, table string) ([]string, error) {
	tc := supportedTables[table]
	if tc.fanOut != "" {
		parents, err := s.collectIDs(ctx, tc.fanOut)
		if err != nil {
			return nil, err
		}
		var ids []string
		for _, p := range parents {
			path := strings.ReplaceAll(tc.path, "{accountHolderId}", p)
			got, err := s.listIDs(ctx, tc, path)
			if err != nil {
				return nil, err
			}
			ids = append(ids, got...)
		}
		return ids, nil
	}
	path := strings.ReplaceAll(tc.path, "{balancePlatform}", s.balancePlatform)
	return s.listIDs(ctx, tc, path)
}

func (s *Source) listIDs(ctx context.Context, tc tableConfig, path string) ([]string, error) {
	var ids []string
	offset := 0
	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		items, hasNext, err := s.getOffsetPage(ctx, tc, path, nil, offset)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if id, ok := it["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
		if !hasNext || len(items) == 0 {
			break
		}
		offset += len(items)
	}
	return ids, nil
}

// getJSON performs one GET and returns the decoded body, failing on any non-2xx.
//
// decoder.UseNumber keeps Adyen's minor-unit amounts and long ids exact: they are
// int64-shaped and float64 would round the largest of them.
func (s *Source) getJSON(ctx context.Context, path string, params map[string]string) (map[string]interface{}, error) {
	req := s.client.R(ctx)
	for k, v := range params {
		req = req.SetQueryParam(k, v)
	}
	resp, err := req.Get(path)
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", path, err)
	}
	if !resp.IsSuccess() {
		return nil, fmt.Errorf("adyen %s returned status %d", path, resp.StatusCode())
	}
	dec := json.NewDecoder(strings.NewReader(string(resp.Body())))
	dec.UseNumber()
	var out map[string]interface{}
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to parse %s response: %w", path, err)
	}
	return out, nil
}

func itemsFrom(body map[string]interface{}, listKey string) []map[string]interface{} {
	raw, ok := body[listKey].([]interface{})
	if !ok {
		return nil
	}
	items := make([]map[string]interface{}, 0, len(raw))
	for _, r := range raw {
		if m, ok := r.(map[string]interface{}); ok {
			items = append(items, m)
		}
	}
	return items
}

// readCursorPaged follows the /btl/v4 style: _links.next carries a full URL whose
// query string must be replayed verbatim.
func (s *Source) readCursorPaged(ctx context.Context, table string, tc tableConfig, path string, params map[string]string, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	p := map[string]string{"limit": fmt.Sprintf("%d", maxPageSize)}
	for k, v := range params {
		p[k] = v
	}
	endpoint := transfersPrefix + path

	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		body, err := s.getJSON(ctx, endpoint, p)
		if err != nil {
			return err
		}
		items := itemsFrom(body, tc.listKey)
		if err := emit(items, tc, opts, results); err != nil {
			return err
		}

		next := nextCursor(body)
		if next == "" {
			return nil
		}
		// The cursor is opaque and arrives inside a full URL; replaying its query
		// verbatim is the only supported way to advance.
		u, err := url.Parse(next)
		if err != nil {
			return fmt.Errorf("adyen %s: unparseable next link: %w", table, err)
		}
		endpoint = u.Path
		p = map[string]string{}
		for k, vs := range u.Query() {
			if len(vs) > 0 {
				p[k] = vs[0]
			}
		}
	}
	config.Debug("[ADYEN] %s hit maxPages guard", table)
	return nil
}

func nextCursor(body map[string]interface{}) string {
	links, ok := body["_links"].(map[string]interface{})
	if !ok {
		return ""
	}
	next, ok := links["next"].(map[string]interface{})
	if !ok {
		return ""
	}
	href, _ := next["href"].(string)
	return href
}

// readOffsetPaged follows the /bcl/v2 style: an explicit offset plus a hasNext flag.
func (s *Source) readOffsetPaged(ctx context.Context, table string, tc tableConfig, path string, params map[string]string, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	offset := 0
	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		items, hasNext, err := s.getOffsetPage(ctx, tc, path, params, offset)
		if err != nil {
			return err
		}
		if err := emit(items, tc, opts, results); err != nil {
			return err
		}
		if !hasNext || len(items) == 0 {
			return nil
		}
		offset += len(items)
	}
	config.Debug("[ADYEN] %s hit maxPages guard", table)
	return nil
}

func (s *Source) getOffsetPage(ctx context.Context, tc tableConfig, path string, params map[string]string, offset int) ([]map[string]interface{}, bool, error) {
	p := map[string]string{"limit": fmt.Sprintf("%d", maxPageSize)}
	for k, v := range params {
		p[k] = v
	}
	if offset > 0 {
		p["offset"] = fmt.Sprintf("%d", offset)
	}
	body, err := s.getJSON(ctx, configPrefix+path, p)
	if err != nil {
		return nil, false, err
	}
	hasNext, _ := body["hasNext"].(bool)
	return itemsFrom(body, tc.listKey), hasNext, nil
}

// emit converts one page to Arrow.
//
// Nested objects are passed through untouched so inference lands them as JSON;
// only the primary key is lifted, since merge needs it as a top-level column.
func emit(items []map[string]interface{}, tc tableConfig, opts source.ReadOptions, results chan<- source.RecordBatchResult) error {
	if len(items) == 0 {
		return nil
	}
	var batch []map[string]interface{}
	var accBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		record, err := arrowconv.ItemsToArrowRecordWithSchema(batch, nil, opts.ExcludeColumns)
		if err != nil {
			return fmt.Errorf("failed to convert %s to Arrow: %w", tc.path, err)
		}
		results <- source.RecordBatchResult{Batch: record}
		batch = nil
		accBytes = 0
		return nil
	}
	for _, row := range items {
		if opts.MaxBatchBytes > 0 {
			rowBytes := arrowconv.RowBytes(row)
			if len(batch) > 0 && accBytes+rowBytes > opts.MaxBatchBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			accBytes += rowBytes
		}
		batch = append(batch, row)
	}
	return flush()
}
