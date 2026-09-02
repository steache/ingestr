# wflow

[wflow](https://www.wflow.com/) is a Czech document-workflow and invoice-approval platform. ingestr reads it through the REST API at `https://api.wflow.com`, authenticating with OAuth2 client credentials.

## URI format

```plaintext
wflow://?client_id=<client_id>&client_secret=<client_secret>
wflow://?client_id=<client_id>&client_secret=<client_secret>&organization=<org_id>
```

URI parameters:

- `client_id`: Required. The OAuth2 client id.
- `client_secret`: Required. That client's secret. URL-encode it if it contains reserved characters.
- `organization`: Optional. Pins a specific organization. Omit it and the organization is discovered from the credential, which is what you normally want — supply it only for a credential that reaches more than one.

### The token endpoint is not the documented one

wflow's published spec advertises `/connect/authorize` as the `tokenUrl`. That is wrong — the OIDC discovery document gives `/connect/token`, which is what this source uses. Following the spec yields an authorization page rather than a token.

## Example usage

```bash
ingestr ingest \
  --source-uri 'wflow://?client_id=<client_id>&client_secret=<client_secret>' \
  --source-table documents \
  --dest-uri duckdb:///wflow.duckdb \
  --dest-table main.documents
```

## Tables

| Table | Primary key | Incremental | Strategy | Data |
|---|---|---|---|---|
| `documents` | `organization`, `id` | `updated` | merge | Invoices and other workflow documents |
| `document_events` | `organization`, `id` | — | merge | Per-document workflow event log |
| `document_files` | `organization`, `id` | — | merge | Attachments |
| `document_approvals` | `organization`, `documentId`, `level` | — | merge | Approval steps |
| `document_comments` | `organization`, `id` | — | merge | Comments |
| `document_links` | `organization`, `id` | — | merge | Links to other documents |
| `document_tags` | `organization`, `id` | — | merge | Tags |
| `registers_<name>` | `organization`, `id` | — | replace | Reference data, see below |

`registers_<name>` covers the `/registers/<name>` collections:

`accountingrules`, `activities`, `businesscases`, `businessitemcategories`, `businessitems`, `carddocumenttypes`, `cashdocumenttypes`, `cashregisters`, `chartofaccounts`, `contracts`, `costcenters`, `employees`, `locations`, `measureunits`, `organizationpersons`, `partnerpersons`, `partners`, `paymentmethods`, `projects`, `series`, `vatcontrolstatementlines`, `vatreturnlines`, `vatreversechargecodes`, `vehicles`

So `--source-table registers_partners` loads `/registers/partners`.

## Notes and limitations

### Every primary key is prefixed with the organization

The API's collections are shared across organizations, so `id` alone is unique only by the accident that wflow uses UUIDs. An org-scoped sequence anywhere would let one organization's row silently replace another's. Every table therefore carries an `organization` column, and it is the first component of every merge key.

### Only `documents` is incremental

`documents` rows carry `updated`, so `--interval-start` narrows the fetch server-side. The register tables carry no timestamp field of any kind, and the document sub-resources are reached through their parent, so both are loaded in full.

That parent relationship is also what bounds the sub-resource cost: on an incremental run the document list is already filtered, so the fan-out covers only documents that actually changed.

### The sub-resources fan out per document

`document_events`, `document_files`, `document_approvals`, `document_comments`, `document_links` and `document_tags` each issue one request per document id. On a first full load of a large organization that is one request per document per table — prefer loading them separately from `documents`, and only the ones you need.

### `document_approvals` has no `id`

It returns approval *steps* (`{level, team, identity, date, status}`) with no identifier of their own; they are unique per document and level. Its primary key is `(organization, documentId, level)` rather than `id`.

### Page size is capped at 100

The API silently caps `pageSize` at 100 — request more and it returns 100 while echoing `pageSize: 100`.
