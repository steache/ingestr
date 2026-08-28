package adyen

import (
	"testing"
	"time"
)

func TestParseURI(t *testing.T) {
	t.Parallel()

	t.Run("happy path defaults to live", func(t *testing.T) {
		cfg, err := parseURI("adyen://?api_key=k&balance_platform=BP123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.apiKey != "k" || cfg.balancePlatform != "BP123" {
			t.Fatalf("bad parse: %+v", cfg)
		}
		if cfg.environment != "live" {
			t.Errorf("environment = %q, want live", cfg.environment)
		}
		if cfg.rateLimit != defaultRateLimit {
			t.Errorf("rateLimit = %v, want %v", cfg.rateLimit, defaultRateLimit)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		cfg, err := parseURI("adyen://?api_key=k&balance_platform=BP&environment=test&rate_limit=2.5")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.environment != "test" || cfg.rateLimit != 2.5 {
			t.Fatalf("overrides not honoured: %+v", cfg)
		}
	})

	for _, tc := range []struct{ name, uri string }{
		{"missing api_key", "adyen://?balance_platform=BP"},
		// Without a scoping id there is no request to make: the transfer endpoints
		// require exactly one of balancePlatform/balanceAccountId/accountHolderId.
		{"missing balance_platform", "adyen://?api_key=k"},
		{"wrong scheme", "stripe://?api_key=k&balance_platform=BP"},
		{"bad environment", "adyen://?api_key=k&balance_platform=BP&environment=sandbox"},
		{"zero rate_limit", "adyen://?api_key=k&balance_platform=BP&rate_limit=0"},
		{"negative rate_limit", "adyen://?api_key=k&balance_platform=BP&rate_limit=-1"},
		{"non-numeric rate_limit", "adyen://?api_key=k&balance_platform=BP&rate_limit=fast"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseURI(tc.uri); err == nil {
				t.Fatalf("expected an error for %s", tc.uri)
			}
		})
	}
}

func TestIsValidTable(t *testing.T) {
	t.Parallel()

	for _, ok := range []string{"transfers", "transactions", "account_holders", "balance_accounts", "payment_instruments"} {
		if !isValidTable(ok) {
			t.Errorf("%q should be a valid table", ok)
		}
	}
	for _, bad := range []string{"", "Transfers", "transfer", "refunds", "payouts", "legal_entities"} {
		if isValidTable(bad) {
			t.Errorf("%q should not be a valid table", bad)
		}
	}
}

// Refunds and payouts are transfer categories, not resources. Exposing them as
// their own tables would re-read and duplicate rows that `transfers` already has.
func TestRefundsAndPayoutsAreNotSeparateTables(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"refunds", "payouts"} {
		if _, exists := supportedTables[name]; exists {
			t.Errorf("%q must not be a table — it is a category of transfers", name)
		}
	}
}

// The transfer endpoints reject a createdSince..createdUntil span wider than six
// months, so no emitted window may exceed maxWindow and the set must cover the
// whole interval without gaps.
func TestWindowsRespectTheAPIMaximum(t *testing.T) {
	t.Parallel()

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, span := range []time.Duration{
		1 * time.Hour,
		30 * 24 * time.Hour,
		maxWindow,
		maxWindow + time.Hour,
		5 * 365 * 24 * time.Hour,
	} {
		end := start.Add(span)
		ws := windows(start, end)
		if len(ws) == 0 {
			t.Fatalf("span %s produced no windows", span)
		}
		if ws[0][0] != start {
			t.Errorf("span %s: first window starts at %s, want %s", span, ws[0][0], start)
		}
		if last := ws[len(ws)-1][1]; last != end {
			t.Errorf("span %s: last window ends at %s, want %s", span, last, end)
		}
		for i, w := range ws {
			if w[1].Sub(w[0]) > maxWindow {
				t.Errorf("span %s: window %d is %s wide, exceeds the API maximum %s", span, i, w[1].Sub(w[0]), maxWindow)
			}
			if i > 0 && ws[i-1][1] != w[0] {
				t.Errorf("span %s: gap between window %d and %d", span, i-1, i)
			}
		}
	}
}

func TestWindowsEmptyWhenEndNotAfterStart(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if ws := windows(at, at); len(ws) != 0 {
		t.Errorf("equal start and end should produce no windows, got %d", len(ws))
	}
	if ws := windows(at, at.Add(-time.Hour)); len(ws) != 0 {
		t.Errorf("end before start should produce no windows, got %d", len(ws))
	}
}

func TestNextCursor(t *testing.T) {
	t.Parallel()

	body := map[string]interface{}{
		"_links": map[string]interface{}{
			"next": map[string]interface{}{"href": "https://x/btl/v4/transfers?cursor=abc"},
		},
	}
	if got := nextCursor(body); got != "https://x/btl/v4/transfers?cursor=abc" {
		t.Errorf("nextCursor = %q", got)
	}
	// A final page omits _links.next entirely; anything else would loop forever.
	for _, last := range []map[string]interface{}{
		{},
		{"_links": map[string]interface{}{}},
		{"_links": map[string]interface{}{"prev": map[string]interface{}{"href": "x"}}},
	} {
		if got := nextCursor(last); got != "" {
			t.Errorf("expected no cursor, got %q", got)
		}
	}
}

func TestItemsFrom(t *testing.T) {
	t.Parallel()

	body := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"id": "a"},
			map[string]interface{}{"id": "b"},
		},
	}
	if got := itemsFrom(body, "data"); len(got) != 2 {
		t.Fatalf("got %d items, want 2", len(got))
	}
	if got := itemsFrom(body, "accountHolders"); got != nil {
		t.Errorf("missing list key should yield nil, got %v", got)
	}
	if got := itemsFrom(map[string]interface{}{"data": "not-a-list"}, "data"); got != nil {
		t.Errorf("non-list should yield nil, got %v", got)
	}
}
