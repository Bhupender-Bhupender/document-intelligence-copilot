# Phase 1 — Cost and Resource Guardrails

## Status

- Phase: 1
- Status: Complete
- Objective: Establish safe Azure and Databricks usage boundaries before provisioning cloud infrastructure.

## Azure Subscription

- Subscription type: Azure Free Account / Free Trial
- Subscription status: Active
- Promotional credit: USD 200 equivalent
- Billing-currency credit observed: approximately GBP 150.51
- Promotional credit expiry: approximately 29 days from verification
- Current Azure cost: 0
- Current forecast: 0
- Spending limit: Credit-based Azure Free Account spending limit
- Existing billable resources: None

## Current Resources

- Resource groups: 0
- Storage accounts: 0
- Azure Databricks workspaces: 0
- AI/Search resources: 0

## Databricks Decision

The Azure Free Trial subscription will not be upgraded yet.

A full Azure Databricks workspace requires a subscription that is not an
Azure Free Trial subscription. The Azure account must therefore be upgraded
to Pay-As-You-Go and its spending limit removed before the full Azure
Databricks environment can be created.

The upgrade will be deferred until the local portability/refactoring work is
complete so that the limited Databricks trial period is not wasted.

Databricks Free Edition may be used as an optional learning sandbox, but it
will not be the primary target environment because it does not support custom
workspace storage locations required for the planned ADLS Gen2 architecture.

## Project Resource Naming Convention

Primary region:
- West Europe

Environment:
- dev

Project identifier:
- docintel

Planned resources:

- Resource group:
  rg-docintel-dev-weu-01

- ADLS Gen2 storage account:
  stdocinteldevweu01

- Azure Databricks workspace:
  adb-docintel-dev-weu-01

- Databricks Access Connector:
  dac-docintel-dev-weu-01

- Key Vault:
  kv-docintel-dev-weu-01

## Standard Resource Tags

- project = document-intelligence
- environment = dev
- workload = data-ai
- purpose = personal-learning
- lifecycle = temporary
- managed-by = manual

## Cost Rules

1. Do not upgrade the Azure subscription until Azure Databricks is required.
2. Do not create always-on compute.
3. Use the smallest practical compute for development.
4. Enable auto-termination wherever supported.
5. Prefer job/serverless compute over persistent interactive compute.
6. Keep native PDFs separate from expensive OCR workloads.
7. Never re-OCR unchanged documents.
8. Never re-embed unchanged chunks.
9. Use local Qwen generation during most development.
10. Create managed AI endpoints only for controlled tests.
11. Delete temporary endpoints and compute after validation.
12. Record every billable resource in the project cost log.
13. Review Azure Cost Analysis after every cloud implementation session.
14. Do not expose subscription IDs, tenant IDs, keys, SAS tokens, or workspace URLs in Git.

## Phase 1 Decision

Cost guardrails established.

Next phase:
Phase 2 — Refactor the local application into a portable production package.
