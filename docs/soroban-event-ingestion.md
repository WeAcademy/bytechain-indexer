# Soroban Event Ingestion — Design Doc

## Overview

The bytechain-indexer is a service that listens to Soroban contract events via the Stellar RPC `getEvents` endpoint and writes them to a database. The backend then reads from this database to stay in sync with on-chain state (DAO proposals, votes, certificate mints, token transfers).

This document covers the event ingestion pipeline, polling strategy, DB write model, error handling, and deployment.

## Architecture

```
Soroban Contract → Stellar Network → RPC Node → [Indexer Poller] → Database ← Backend API
                                                     ↓
                                              [Event Processor]
```

The indexer runs as a standalone Node.js/NestJS service that:
1. Polls the Stellar RPC `getEvents` endpoint at a fixed interval
2. Parses contract events into typed records
3. Writes them to a PostgreSQL database with idempotency
4. Exposes a REST API for the backend to query synced state

## Event sources

| Contract | Events | Purpose |
|----------|--------|---------|
| ByteChainDAO | `ProposalCreated`, `VoteCast`, `ProposalResolved`, `ProposalWithdrawn`, `ProposalModerated` | Sync DAO state to backend |
| ByteChainCertificate | `Mint`, `Revoke` | Sync certificate issuance/revocation |
| ByteChainToken | `Transfer`, `Mint` | Sync token balances and reward history |

## Polling strategy

- **Poll interval**: 5 seconds (configurable via `POLL_INTERVAL_MS`)
- **Cursor**: The indexer maintains a `latest_ledger` cursor in the database. Each poll request uses `startLedger = latest_ledger + 1`.
- **Batch size**: Up to 100 events per request (RPC limit).
- **Idempotency**: Each event has a unique `transaction_hash + event_index` composite key. Re-processing the same event is a no-op (INSERT ... ON CONFLICT DO NOTHING).

## RPC getEvents usage

```typescript
const response = await rpcServer.getEvents({
  startLedger: lastProcessedLedger + 1,
  endLedger: currentLedger,
  filters: [
    {
      type: 'contract',
      contractIds: [DAO_CONTRACT_ID, CERTIFICATE_CONTRACT_ID, TOKEN_CONTRACT_ID],
    },
  ],
  limit: 100,
});
```

## DB write model

### `indexed_events` table (raw event log)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | Primary key |
| `ledger` | BIGINT | Ledger number |
| `transaction_hash` | VARCHAR(64) | Transaction hash |
| `event_index` | INT | Event index within transaction |
| `contract_id` | VARCHAR(56) | Soroban contract ID |
| `event_type` | VARCHAR(64) | Parsed event type (e.g., `ProposalCreated`) |
| `event_data` | JSONB | Raw event topics and data |
| `created_at` | TIMESTAMP | Ingestion timestamp |
| **Unique** | `(transaction_hash, event_index)` | Idempotency constraint |

### `synced_proposals` table (processed DAO proposals)

| Column | Type | Notes |
|--------|------|-------|
| `id` | BIGINT | On-chain proposal ID |
| `title` | TEXT | Proposal title |
| `description` | TEXT | Proposal description |
| `proposer` | VARCHAR(56) | Proposer's Stellar address |
| `status` | VARCHAR(20) | ACTIVE / PASSED / REJECTED / WITHDRAWN |
| `voting_deadline` | TIMESTAMP | Deadline |
| `yes_power` | NUMERIC | Sum of YES voting power |
| `no_power` | NUMERIC | Sum of NO voting power |
| `abstain_power` | NUMERIC | Sum of ABSTAIN voting power |
| `created_at` | TIMESTAMP | On-chain creation time |
| `moderation_reason` | TEXT | Nullable |
| `moderated_at` | TIMESTAMP | Nullable |
| `moderator` | VARCHAR(56) | Nullable |
| `updated_at` | TIMESTAMP | Last sync time |

### `synced_votes` table (processed DAO votes)

| Column | Type | Notes |
|--------|------|-------|
| `proposal_id` | BIGINT | FK to synced_proposals |
| `voter` | VARCHAR(56) | Voter's Stellar address |
| `vote` | VARCHAR(10) | YES / NO / ABSTAIN |
| `voting_power` | NUMERIC | BCT balance at vote time |
| `cast_at` | TIMESTAMP | On-chain vote time |
| **PK** | `(proposal_id, voter)` | One vote per address per proposal |

### `synced_certificates` table (processed certificate events)

| Column | Type | Notes |
|--------|------|-------|
| `token_id` | BIGINT | On-chain token ID |
| `owner` | VARCHAR(56) | Owner's Stellar address |
| `course_id` | VARCHAR(64) | Hashed backend course UUID |
| `recipient_name` | TEXT | Student name |
| `course_name` | TEXT | Course title |
| `token_uri` | TEXT | IPFS URI |
| `issued_at` | TIMESTAMP | Mint time |
| `revoked` | BOOLEAN | Revocation status |
| `revoked_reason` | TEXT | Nullable |
| `updated_at` | TIMESTAMP | Last sync time |

### `ingestion_cursor` table (polling state)

| Column | Type | Notes |
|--------|------|-------|
| `id` | INT | Always 1 (singleton) |
| `latest_ledger` | BIGINT | Last processed ledger |
| `updated_at` | TIMESTAMP | Last poll time |

## Event processing pipeline

1. **Fetch**: Poll RPC `getEvents` with the cursor's `latest_ledger + 1` as `startLedger`
2. **Parse**: For each event, extract the event type from the first topic, decode the data
3. **Deduplicate**: Check `(transaction_hash, event_index)` uniqueness
4. **Transform**: Map raw event data to the appropriate synced table
5. **Write**: Upsert into the synced table (ON CONFLICT DO UPDATE)
6. **Advance cursor**: Update `latest_ledger` to the last ledger in the batch

## Error handling

- **RPC timeout**: Retry with exponential backoff (3 retries, 1s/2s/4s)
- **RPC rate limit**: Respect 429 responses with Retry-After header
- **Parse error**: Log the raw event and continue (don't block the pipeline)
- **DB write error**: Retry the batch; if persistent, dead-letter the event to `failed_events` table
- **Ledger gap**: If `startLedger` is too old (> 7 days for public RPC), restart from the latest ledger and log a warning

## Deployment

- **Runtime**: Node.js 20+ (NestJS or standalone script)
- **Database**: PostgreSQL 16 (shared with or separate from backend)
- **Environment variables**: `STELLAR_RPC_URL`, `DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME`, `DAO_CONTRACT_ID`, `CERTIFICATE_CONTRACT_ID`, `TOKEN_CONTRACT_ID`, `POLL_INTERVAL_MS`
- **Process management**: PM2 or Docker container with restart policy

## Health checks

- `GET /health` — returns `{ status: "ok", latestLedger: N, eventsProcessed: M, lastPollAt: ISO }`
- If `lastPollAt` is more than 60 seconds ago, health check returns 503

## Future extensions

- WebSocket push to backend (instead of polling the DB)
- Support for multiple networks (testnet + mainnet simultaneously)
- Historical backfill mode (process events from a specific start ledger)
- Event subscription via Mercury or Tigerbeetle for higher throughput
