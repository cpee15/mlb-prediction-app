# MLBGPT Control Center Phase 2

Issue #1175 extends the owner-only Control Center foundation from #1167. It adds private user-directory metadata, code-validated settings and feature flags, and immutable administrative audit history. It does not add Permission Sets, custom roles, operational execution, federation login, or Workbench execution.

## Additive production schema

The deployed `app_users` table is not altered. Phase 2 adds independent tables that SQLAlchemy can create safely:

- `app_user_directory_profiles` extends an existing user with an immutable public UUID, display metadata, locale/language/timezone, active/locked state, last-login time, and a session-revocation version;
- `federated_identities` reserves issuer/subject identity mappings with a unique issuer + subject constraint and contains no passwords, tokens, or secrets;
- `app_access_profiles` persists catalog identity for the code-owned `Owner Administrator` and `Standard User` profiles;
- `app_global_settings` and `app_user_settings` provide typed configuration storage without changing authorization;
- `app_feature_flags` stores only the four registered foundation flags and their validated profile targets;
- `app_admin_audit_events` records immutable safe before/after summaries for successful administrative mutations;
- `app_login_history` records successful password-backed session issuance without recording credentials or tokens.

The existing `app_user_roles` assignment remains the role source of truth. Profile labels map onto that role/capability registry; they do not create another authorization framework.

## Private APIs

Phase 2 keeps the existing read-only overview, object, app, and user-list routes and adds:

```text
GET   /admin/me
GET   /admin/users/{user_id}
PATCH /admin/users/{user_id}
GET   /admin/profiles
GET   /admin/settings
PATCH /admin/settings
GET   /admin/feature-flags
PATCH /admin/feature-flags
GET   /admin/audit-events
```

Every route uses a server-owned capability dependency. `admin.users.manage` protects safe directory changes, `admin.settings.manage` protects settings and flag changes, and `admin.audit.read` protects audit history. A modified frontend capability array has no authorization effect.

User updates reject email, password, password hash, role, capability, profile assignment, plan, session, token, secret, arbitrary preferences, and saved-report fields. An authenticated owner cannot deactivate or lock itself. Active/locked changes increment the revocation marker and delete every existing session for the affected account.

## Settings and feature flags

The initial global settings are directory-profile defaults for locale, language, and timezone. The server registry owns their types, allowed values, defaults, and descriptions. Unknown keys and invalid values are rejected.

The initial feature flags are:

- `federation_enabled`
- `federation_admin_enabled`
- `workbench_query_enabled`
- `federation_refresh_enabled`

All four resolve to false without a stored override. Target profiles must be `owner_administrator` or `standard_user`. A flag never grants a capability and none of these flags is connected to public product behavior in this phase.

Plaintext secret values are not accepted. Future sensitive configuration must use an environment-backed or external secret reference and must never be serialized through these APIs.

## Audit boundary

Successful user, setting, and feature-flag changes record actor user/session IDs, action, target, safe before/after summaries, source, and timestamp. Audit records have no update or delete API. The serializer excludes password, session-token, token, secret, and sensitive-reference fields.

## Frontend boundary

The existing private `/admin` page now supports:

- safe user-directory editing and account-state controls;
- a read-only Owner Administrator / Standard User profile catalog;
- allowlisted global settings;
- default-off feature flags with profile targeting;
- read-only audit history.

Operations and Workbench remain locked. There is still no public Admin navigation entry.

## Next implementation phase

The next ticket should define the constrained MLBGPT Workbench request language and compiler over the existing object/field registry. It must accept structured object, field, filter, grouping, aggregate, weight, sort, pagination, date, and relationship inputs; reject arbitrary SQL and physical table names; use bound parameters; enforce cost and row limits; and preserve saved-report compatibility. Permission Sets and delegated role administration remain a later separately reviewed phase.
