package wflow

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpclient "github.com/bruin-data/ingestr/pkg/http"
)

// An API error body must NEVER decode to "zero rows, no error" — that is silent
// data loss: the caller sees len(items)==0 and skips the document.
func TestDecodeItemsRejectsErrorBodies(t *testing.T) {
	for _, body := range []string{
		`{"message":"Too Many Requests"}`,
		`{"error":"internal","traceId":"abc"}`,
		`{"title":"Forbidden","status":403}`,
	} {
		if _, err := decodeItems([]byte(body)); err == nil {
			t.Errorf("decodeItems(%s) returned no error — document would be silently skipped", body)
		}
	}
}

// ...but a genuinely empty collection is still valid, in both shapes.
func TestDecodeItemsAcceptsEmptyCollections(t *testing.T) {
	for _, body := range []string{`[]`, `{"items":[]}`, `{"pathName":"x","items":[]}`} {
		items, err := decodeItems([]byte(body))
		if err != nil {
			t.Errorf("decodeItems(%s) errored on a legitimately empty collection: %v", body, err)
		}
		if len(items) != 0 {
			t.Errorf("decodeItems(%s) = %d items, want 0", body, len(items))
		}
	}
}

// getJSON must surface a non-2xx as an error rather than handing the error body
// to the decoder.
func TestGetJSONRejectsNon2xx(t *testing.T) {
	for _, code := range []int{403, 404} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"message":"nope"}`))
		}))
		s := newTestSource(t, srv.URL)
		if _, err := s.getJSON(t.Context(), "/whatever"); err == nil {
			t.Errorf("getJSON did not error on HTTP %d", code)
		} else if !strings.Contains(err.Error(), "nope") {
			t.Errorf("HTTP %d error should carry the body, got: %v", code, err)
		}
		srv.Close()
	}
}

// A 429 must be retried, not dropped.
func TestGetJSONRetries429(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"slow down"}`))
			return
		}
		_, _ = w.Write([]byte(`[{"id":"1"}]`))
	}))
	defer srv.Close()
	s := newTestSource(t, srv.URL)
	body, err := s.getJSON(t.Context(), "/documents")
	if err != nil {
		t.Fatalf("429 was not retried: %v", err)
	}
	var got []map[string]any
	if e := json.Unmarshal(body, &got); e != nil || len(got) != 1 {
		t.Fatalf("retry returned wrong body: %s", body)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (429 then success), got %d", calls)
	}
}

// newTestSource points the source at a stub server and pre-loads a token so
// ensureAccessToken does not try to reach the real OAuth endpoint.
func newTestSource(t *testing.T, baseURL string) *WflowSource {
	t.Helper()
	return &WflowSource{
		client: httpclient.New(
			httpclient.WithBaseURL(baseURL),
			httpclient.WithTimeout(5*time.Second),
			httpclient.WithHeader("Accept", "application/json"),
		),
		accessToken:  "test-token",
		tokenExpiry:  time.Now().Add(1 * time.Hour),
		organization: "test-org",
	}
}
