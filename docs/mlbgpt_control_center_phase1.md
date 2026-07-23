# MLBGPT Control Center Phase 1

This document records the security and deployment contract for issue #1167, Phase 1 of #1156. The Control Center is an owner-only, read-only administrative surface at `/admin`. It does not add mutable settings, Permission Sets, destructive operations, or the advanced Workbench compiler.

## Authentication correction

The former combined `POST /my-dashboard/profile` path could create a session for an existing password-backed account when the password was omitted. It could also create passwordless accounts and let a legacy passwordless account be claimed by submitting a new password.

Phase 1 keeps that endpoint for frontend compatibility but changes its behavior:

- existing accounts must supply and verify their existing password before any profile mutation or session creation;
- legacy passwordless accounts receive an explicit recovery-required response and cannot be claimed through email knowledge;
- new accounts require a password of at least eight characters;
- explicit register, login, and logout endpoints now own the normal frontend flow;
- client-supplied role, capability, administrator, and ownership fields are rejected;
- plan display values and profile preferences never participate in authorization;
- sessions retain the existing six-hour lifetime.

The explicit endpoints are:

```text
POST /my-dashboard/auth/register
POST /my-dashboard/auth/login
POST /my-dashboard/auth/logout
```

No recovery mechanism is invented in this phase. A separate recovery design must establish ownership using a channel stronger than knowledge of the account email.

## Owner bootstrap

Roles live in the additive `app_user_roles` table instead of a new column on `app_users`. This is safe for an existing deployment because SQLAlchemy `create_all()` can create the new table without pretending it can alter the deployed `app_users` schema. Each assignment records its role, assignment source, assignment time, verification time, and update time.

Owner bootstrap uses the normalized, comma/semicolon/whitespace-separated `MLBGPT_ADMIN_EMAILS` deployment allowlist. It never promotes the first registered account.

Use this order for a new owner:

1. With the email absent from `MLBGPT_ADMIN_EMAILS`, create the password-backed MyDashboard account normally.
2. Sign out.
3. Add that normalized email to `MLBGPT_ADMIN_EMAILS` and restart the service.
4. Sign in again with the account password.

An allowlisted email cannot be registered while already allowlisted. This prevents someone who only knows the configured owner email from choosing its initial password. The role is granted only after a verified login to an existing password-backed account. On the first grant, all older sessions for that account are revoked; the verified login then receives a fresh session. A session created before the role assignment cannot resolve administrator capabilities.

Removing an email from the deployment allowlist does not silently rewrite an already recorded role assignment. Role revocation and manual role management belong to a later audited administration phase.

## Server-owned capabilities

All capability resolution happens on the server. The frontend may hide an entry point, but every administrative endpoint independently resolves the session and enforces its required capability.

Standard users receive report execution, personal saved-report/folder management, sorting, filtering, pagination, and export capabilities. Administrators extend that set with:

```text
admin.portal.access
admin.objects.read
admin.apps.read
admin.users.read
admin.settings.read
admin.operations.read
admin.audit.read
workbench.advanced
```

Unauthenticated requests receive `401`; authenticated users without the required capability receive `403`. Role and sorted capabilities are additive fields in both the authenticated profile and workspace contracts.

## Read-only administrative APIs

The administrative router is mounted directly by `mlb_app/app.py`, not nested under the AI Data Assistant router.

```text
GET /admin/overview
GET /admin/objects
GET /admin/apps
GET /admin/users
```

- Overview returns the verified administrator identity, resolved role/capabilities, dynamic registry counts, a sanitized hydration summary, and locked next-phase sections.
- Object Manager returns the existing `dashboard_report_types.py` contracts and field catalogs. It does not expose unrestricted physical schema inspection or a second frontend-owned object schema.
- Apps returns a code-owned registry of application surfaces; it does not scrape React source at runtime.
- Users returns only explicit safe identity, role, plan display value, capabilities, and timestamps. Password hashes, sessions, tokens, unrestricted preferences, and saved-report contents are excluded. No account-status model is invented in this phase.

Settings, Operations, Workbench, and Audit are visible as intentional locked states. Their capability names reserve the authorization boundary but do not make those areas mutable.

## Existing route audit and production blocker

The following existing routes remain public in this phase:

- report metadata and health: `GET /my-dashboard/health`, `GET /my-dashboard/report-types`;
- canonical and hydration status: `GET /my-dashboard/canonical/status`, `GET /my-dashboard/hydration/status`;
- report execution: `POST /my-dashboard/reports/query`;
- solver routes: `GET/POST /my-dashboard/solver`, `POST /my-dashboard/solver/batch`, and `POST /my-dashboard/solver/active-lineups`;
- hydration execution: `GET/POST /my-dashboard/solver/hydrate-yesterday`.

Repository caller audit:

- `MyDashboardReportBuilderPage.jsx` reads `/my-dashboard/report-types`.
- The canonical production runbook references `/my-dashboard/canonical/status` and `/my-dashboard/report-types` for operator inspection.
- The Workbench acceptance document references `/my-dashboard/solver/hydrate-yesterday` for forced verification.
- `my_dashboard_observability.py` records the hydration endpoint as an observability target string; it does not invoke it.
- No checked-in frontend, Python caller, GitHub workflow, Railway service definition, or cron configuration invokes the hydration execution endpoint.
- The suggested canonical refresh schedule runs `scripts/refresh_dashboard_player_projection.py --refresh`; it does not call the hydration endpoint.

Protecting legacy operational routes therefore requires identifying the production scheduler outside this repository, assigning a scoped cron/service credential, deploying that credential to every real caller, and adding compatibility monitoring before enforcement. That is an explicit follow-up. Phase 1 does not break an unknown production schedule or claim that cron authentication is complete.

The stale persistence label on public `/my-dashboard/health` is also left unchanged. Correcting it remains a narrowly scoped compatibility-tested metadata change.

## Immediate follow-up boundaries

The next phases should build on this role/capability foundation rather than extending the older unrouted MyDashboard Workspace or Workbench pages:

1. Define audited role assignment and Permission Set semantics, including revocation and owner safeguards.
2. Protect operational routes with a deployment-ready service credential after production callers are identified.
3. Define the constrained Workbench report language, allowlisted object/field/operator grammar, compiler, cost limits, and read-only execution contract before exposing advanced report authoring.

None of those concerns are implemented by Phase 1.
