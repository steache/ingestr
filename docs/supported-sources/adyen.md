# Adyen

[Adyen](https://www.adyen.com/) is a global payments platform. This source reads Adyen's **Balance Platform** — the product used by marketplaces and platforms to move money between account holders.

ingestr supports Adyen through the [Balance Platform Transfers API v4](https://docs.adyen.com/api-explorer/transfers/4/overview) and the [Balance Platform Configuration API v2](https://docs.adyen.com/api-explorer/balanceplatform/2/overview).

## URI format

```plaintext
adyen://?api_key=<api_key>&balance_platform=<id>
adyen://?api_key=<api_key>&balance_platform=<id>&environment=test
```

URI parameters:

- `api_key`: Required. A Balance Platform API key (`X-API-Key`).
- `balance_platform`: Required, never defaulted. The balance platform identifier that scopes every request.
- `environment`: Optional. `live` (default) or `test`. These are **different hostnames**, not a path.
- `rate_limit`: Optional. Requests per second, default `8`.

### Keys are scoped per API

An Adyen API key is scoped to one API *and* one account. A Balance Platform key returns `403` on Legal Entity Management endpoints, and a Legal Entity Management key returns `401` on Balance Platform. A `401` therefore often means "right key, wrong API" rather than a bad credential.

This source covers Balance Platform only. Legal Entity Management is not included: it needs a separate key, and its payloads carry personal data (names, email addresses, phone numbers, addresses) that transfer data does not.

## Example usage

```bash
ingestr ingest \
  --source-uri 'adyen://?api_key=<api_key>&balance_platform=<id>' \
  --source-table transfers \
  --dest-uri duckdb:///adyen.duckdb \
  --dest-table main.transfers
```

## Tables

| Table | Primary key | Incremental | Strategy | Data |
|---|---|---|---|---|
| `transfers` | `id` | `createdAt` | merge | Money movement — payments, refunds, payouts, internal transfers |
| `transactions` | `id` | `creationDate` | merge | Balance-account ledger entries for those movements |
| `account_holders` | `id` | — | merge | Account holders on the platform |
| `balance_accounts` | `id` | — | merge | Balance accounts, with `account_holder_id` |
| `payment_instruments` | `id` | — | merge | Cards and bank accounts issued to balance accounts |

`transfers` and `transactions` accept `--interval-start` / `--interval-end`. The rest are snapshots fetched in full.

### Joining

A transfer carries `account_holder` and `balance_account` as JSON objects holding `{id, description}`, so the join path is:

```
transfers.balance_account.id → balance_accounts.id
balance_accounts.account_holder_id → account_holders.id
```

Nested API objects are passed through as JSON rather than flattened, so extract with your destination's JSON functions.

## Notes and limitations

### Refunds and payouts are not separate tables

Both are rows in `transfers`, distinguished by `category`:

| category | meaning |
|---|---|
| `platformPayment` | a payment or refund |
| `bank` | a payout |
| `card`, `internal`, `issuedCard`, `topUp` | other movement types |

Exposing them as their own tables would re-read and duplicate rows `transfers` already has.

### The date window is capped at six months

`/transfers` and `/transactions` **require** both `createdSince` and `createdUntil`, and reject any span wider than six months. There is no unbounded listing.

The source splits whatever interval it is given into windows no larger than that, so a multi-year backfill works — it simply costs one page-walk per window. When no interval is given, it starts from a fixed historical floor rather than letting the API pick a default, so "no interval" means everything rather than a recent slice.

### There is no search by payment reference

`/transfers` cannot filter by `pspReference`, a merchant reference, or a shopper email. Its `reference` filter matches only a reference supplied when creating a transfer, which is not useful for reading. `/transactions` is stricter still — it rejects `category` and `reference` outright with a `422`, accepting only the scoping id, the created window, and paging.

Filter after loading rather than trying to push these predicates into the API.

### The two APIs paginate differently

Transfers and transactions use cursor pagination (`_links.next`); the configuration tables use offset pagination (`hasNext` / `hasPrevious`). The source handles both, but it is worth knowing if you extend it.

### `payment_instruments` is expensive

It has no platform-wide listing: it fans out one request per balance account, after first walking every account holder to collect those ids. On a platform with a few thousand account holders that is several thousand sequential requests. Prefer loading it separately from the other tables, and only when you need it.
