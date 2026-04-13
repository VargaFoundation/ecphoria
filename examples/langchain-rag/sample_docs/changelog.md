# NovaCRM — Changelog

## v3.8.0 (2024-12-15)

### New Features
- **AI-powered deal scoring**: Automatic deal probability scoring based on historical win/loss patterns and engagement signals. Available on Pro and Enterprise plans.
- **Custom dashboard widgets**: Build and share custom dashboard widgets using our new widget SDK. Supports charts, tables, and KPI cards.
- **Bulk email campaigns**: Send personalized email campaigns to contact segments directly from NovaCRM. Includes open/click tracking and A/B testing.

### Improvements
- Contact search is now 3x faster on accounts with more than 100,000 contacts.
- API pagination now supports cursor-based pagination in addition to offset-based.
- Webhook delivery logs now show full request/response bodies for debugging.

### Bug Fixes
- Fixed an issue where deal stage changes were not triggering workflow automations.
- Fixed timezone handling in activity timestamps for users in UTC+ timezones.
- Fixed CSV export failing for contacts with special characters in custom fields.

## v3.7.0 (2024-10-01)

### New Features
- **Workflow templates**: Pre-built automation templates for common scenarios (lead nurturing, onboarding, renewal reminders). One-click setup.
- **Microsoft Teams integration**: Receive CRM notifications in Teams channels and create contacts from Teams conversations.
- **API rate limit dashboard**: Real-time visibility into your API usage and remaining quota. Available under Settings > API.

### Improvements
- Reduced email sync latency from 5 minutes to under 30 seconds.
- Added support for custom date fields in contact filters and saved views.
- Improved error messages for API validation errors (now includes field-level details).

### Bug Fixes
- Fixed duplicate webhook deliveries when multiple automations triggered on the same event.
- Fixed contact merge losing custom field values from the secondary contact.

## v3.6.0 (2024-08-01)

### New Features
- **Audit log API**: Programmatic access to all user actions via `/api/v1/audit-logs`. Enterprise plan only.
- **Contact enrichment**: Automatic company and social profile enrichment for new contacts using Clearbit integration.

### Improvements
- Dashboard loading time improved by 40% through query optimization.
- Added bulk delete endpoint (`DELETE /api/v1/contacts/bulk`) for GDPR data removal requests.

### Bug Fixes
- Fixed SSO login redirect loop when session expired during a long form submission.
- Fixed deal pipeline drag-and-drop not working in Firefox.
- Fixed activity feed showing events from archived contacts.
