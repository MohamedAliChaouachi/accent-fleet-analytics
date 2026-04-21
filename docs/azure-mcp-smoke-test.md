# Azure MCP Smoke Test — Resource Group Listing

**Date:** 2026-04-21  
**Subscription ID:** `20d029eb-853f-4df8-9fc8-8bc8b982aaf6`

---

## Objective

Verify that Azure MCP tools can successfully authenticate and enumerate resource groups within the target subscription.

---

## Results

The Azure MCP `group_list` tool was executed against subscription `20d029eb-853f-4df8-9fc8-8bc8b982aaf6`.

**Status:** ✅ Success (HTTP 200)

### Discovered Resource Groups

| Name | Location | Resource ID |
|---|---|---|
| `datawarehouse-rg` | `westeurope` | `/subscriptions/20d029eb-853f-4df8-9fc8-8bc8b982aaf6/resourceGroups/datawarehouse-rg` |
| `NetworkWatcherRG` | `westeurope` | `/subscriptions/20d029eb-853f-4df8-9fc8-8bc8b982aaf6/resourceGroups/NetworkWatcherRG` |

**Total resource groups found:** 2

---

## Permission & MCP Configuration Notes

- **Authentication:** Successful — the MCP tool resolved credentials without any explicit key or connection string (default `Credential` method via Azure CLI / managed identity).
- **Permissions:** The caller has at least `Reader` scope on the subscription; no `AuthorizationFailed` or `403` errors were returned.
- **Blockers:** None detected. Both resource groups were returned in a single call with no pagination required.

---

## Relevant Infrastructure

| Resource Group | Purpose |
|---|---|
| `datawarehouse-rg` | Likely hosts the data warehouse infrastructure used by this pipeline (PostgreSQL / Synapse / etc.) |
| `NetworkWatcherRG` | Auto-created by Azure when Network Watcher is enabled in the region; not project-specific |
