# Pebble Monorepo Snapshot

This repository contains a snapshot of the Pebble monorepo. Deployment scripts are not included.

## Overview

Pebble is a high throughput, low-latency exchange platform focusing on outcome trading, built on the Canton/DAML ledger. It features a modular and hybrid architecture to ensure scalability, privacy, and auditability.

## Project Layout

### Apps (`apps/`)

| Directory       | Description                                                        |
| --------------- | ------------------------------------------------------------------ |
| `apps/api`      | API server handling orders, fills, accounts, and market operations |
| `apps/assets`   | Image asset service with on-the-fly resizing and LRU caching       |
| `apps/clearing` | Epoch-based netting and atomic settlement of fill deltas to Canton |
| `apps/indexer`  | Streams Canton/DAML events and projects on-ledger state into DB    |
| `apps/treasury` | On-ledger mint/burn orchestration and token-rail reconciliation    |
| `apps/web`      | Main app UI built with React                                       |

### Crates (`crates/`)

| Directory              | Description                                                                        |
| ---------------------- | ---------------------------------------------------------------------------------- |
| `crates/daml-grpc`     | Generated tonic/gRPC stubs for the DAML Ledger API v2                              |
| `crates/ids`           | Typed identifier parsing and validation (account, market, deposit, withdrawal IDs) |
| `crates/ledger-client` | Async high-level client wrapper over `daml-grpc` for Canton commands               |
| `crates/trading`       | Order matching engine and shared domain types (orders, fills, risk state)          |

### Contracts (`contracts/`)

| Directory                            | Description                                                                                                                    |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `contracts/pebble`                   | Core DAML templates for accounts, markets, orders, and settlement                                                              |
| `contracts/pebble-scripts`           | DAML Scripts for ledger setup, initialization, and admin operations                                                            |
| `contracts/wizardcat-token-standard` | Simplified fungible token standard based on CIP-56, used for on-ledger cash (will be replaced with real CIP-56 for production) |

### Infrastructure (`infra/`)

| Directory      | Description                                                |
| -------------- | ---------------------------------------------------------- |
| `infra/canton` | Local Canton node configuration for development            |
| `infra/docker` | Docker Compose files for Postgres and full local dev stack |
| `infra/sql`    | Versioned SQL migration scripts for the off-ledger schema  |

### Other

| Directory        | Description                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------ |
| `proto/`         | Raw `.proto` sources for the DAML Ledger API gRPC interface                                |
| `tools/scripts/` | Deployment scripts and scripted trading simulation scenarios for API/engine stress testing |
| `data/assets/`   | Static image assets (thumbnails, cards, heroes) for scripted markets                       |

## License

Copyright © 2026 [Lumo](https://github.com/lumosimmo). All rights reserved.

This software is proprietary. No part of this repository may be reproduced, distributed, or transmitted in any form without prior written permission from the copyright holder.
