# bytechain-indexer

An on-chain event listener service for **ByteChain Academy** — watches deployed smart contracts and syncs blockchain state (DAO votes, minted certificates, token transfers) to the backend database so the platform stays in sync with the chain without polling.

## Part of the ByteChain Academy multi-repo ecosystem

| Repo | Purpose |
|------|---------|
| [bytechain-academy-frontend](https://github.com/WeAcademy/bytechain-academy-frontend) | Next.js web app — UI for students, tutors, and admins |
| [bytechain-academy-backend](https://github.com/WeAcademy/bytechain-academy-backend) | NestJS REST API — auth, courses, progress, DAO, certificates |
| [bytechain-currency-hub](https://github.com/WeAcademy/bytechain-currency-hub) | Standalone crypto currency history data service |
| [bytechain-contracts](https://github.com/WeAcademy/bytechain-contracts) | Smart contracts — DAO governance, SBT certificates, reward token |
| **bytechain-indexer** (this repo) | On-chain event listener — syncs chain state to the backend DB |
| [bytechain-shared](https://github.com/WeAcademy/bytechain-shared) | Shared TypeScript types and contract ABIs used across all repos |

---

## What this service does

ByteChain's smart contracts emit events every time something meaningful happens on-chain — a DAO vote is cast, a certificate SBT is minted, a reward token is transferred. The indexer listens for those events in real time and writes the results into the backend database so the NestJS API can serve them instantly without ever querying the blockchain directly.

Without the indexer, the backend would either need to query the chain on every API request (slow, expensive) or run on stale data. The indexer solves this by keeping the database as a live, queryable mirror of on-chain state.

---

## Events indexed (planned)

### ByteChainDAO contract
| Event | Action taken |
|-------|-------------|
| `ProposalCreated` | Creates a new proposal record in the database |
| `VoteCast` | Increments yes/no/abstain vote counts, records voter address |
| `ProposalExecuted` | Updates proposal status to PASSED and triggers any execution hooks |
| `ProposalCancelled` | Updates proposal status to CANCELLED |

### ByteChainCertificate (SBT) contract
| Event | Action taken |
|-------|-------------|
| `CertificateMinted` | Records token ID, wallet address, course ID, and IPFS metadata URI |
| `MetadataUpdated` | Updates the stored metadata URI for the certificate |

### ByteChainToken (BCT) contract
| Event | Action taken |
|-------|-------------|
| `Transfer` | Records token transfers for reward history and leaderboard updates |
| `RewardIssued` | Updates user XP balance in the backend |

---

## Planned tech stack

| Layer | Technology |
|-------|-----------|
| Runtime | Node.js 20+, TypeScript |
| Chain listening (Stellar) | Stellar RPC `getEvents` — Soroban contract event ingestion |
| Database | PostgreSQL (same instance as backend, separate schema) |
| Queue | Redis (optional — for buffering high-volume events) |
| Restart safety | Persists last processed block number to resume after restarts |
| Deployment | Docker, Railway, or Fly.io |

> **Chain path decision:** ByteChain uses Stellar Soroban. This service polls Stellar RPC `getEvents` for contract events; it does not use EVM `viem` or Horizon server-sent events. See [`docs/event-ingestion.md`](docs/event-ingestion.md) for the ingestion and DB write model design.
>
> ---
>
> ## Repository structure (planned)
>
> ```
> bytechain-indexer/
> ├── src/
> │   ├── listeners/
> │   │   ├── dao.listener.ts          ← Watches DAO contract events
> │   │   ├── certificate.listener.ts  ← Watches SBT mint events
> │   │   └── token.listener.ts        ← Watches reward token events
> │   ├── handlers/
> │   │   ├── dao.handler.ts           ← Writes DAO events to DB
> │   │   ├── certificate.handler.ts   ← Writes cert events to DB
> │   │   └── token.handler.ts         ← Writes token events to DB
> │   ├── db/
> │   │   └── client.ts                ← PostgreSQL connection
> │   ├── config.ts                    ← Chain RPC URLs, contract addresses
> │   └── main.ts                      ← Entry point — starts all listeners
> ├── .env.example
> ├── Dockerfile
> ├── package.json
> └── tsconfig.json
> ```
>
> ---
>
> ## Getting started (once implemented)
>
> ### Prerequisites
>
> - Node.js 20+
> - - PostgreSQL (shared with backend or separate)
>   - - Deployed contracts from [bytechain-contracts](https://github.com/WeAcademy/bytechain-contracts)
>    
>     - ### Install and run
>    
>     - ```bash
>       git clone https://github.com/WeAcademy/bytechain-indexer.git
>       cd bytechain-indexer
>       cp .env.example .env
>       # Fill in contract addresses and RPC URL
>       npm install
>       npm run start
>       ```
>
> ---
>
> ## Environment variables
>
> ```bash
> # Chain connection
> RPC_URL=                        # e.g. https://mainnet.infura.io/v3/YOUR_KEY
>                                 # or https://horizon.stellar.org for Stellar
>
> # Contract addresses (from bytechain-contracts deployment)
> DAO_CONTRACT_ADDRESS=
> CERTIFICATE_CONTRACT_ADDRESS=
> TOKEN_CONTRACT_ADDRESS=
>
> # Database
> DATABASE_URL=                   # PostgreSQL connection string
>
> # Start block (resume from this block on restart)
> START_BLOCK=0
> ```
>
> ---
>
> ## How it connects to the backend
>
> The indexer **writes directly to the same PostgreSQL database** that the NestJS backend reads from. It does not call the backend's REST API. This keeps the architecture simple and avoids circular dependencies.
>
> ```
> Blockchain
>     ↓ (on-chain events)
> bytechain-indexer
>     ↓ (writes to DB)
> PostgreSQL
>     ↑ (reads from DB)
> bytechain-academy-backend
>     ↑ (REST API calls)
> bytechain-academy-frontend
> ```
>
> ---
>
> ## Contributing
>
> See [CONTRIBUTING.md](https://github.com/WeAcademy/ByteChain-Academy/blob/main/CONTRIBUTING.md) for branch naming, PR standards, and the Drips Wave contribution workflow.
>
> ## License
>
> MIT
