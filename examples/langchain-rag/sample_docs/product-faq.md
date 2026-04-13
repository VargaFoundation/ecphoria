# NovaCRM — Frequently Asked Questions

## What is NovaCRM?

NovaCRM is a customer relationship management platform designed for growing SaaS companies. It combines contact management, deal tracking, automated workflows, and built-in analytics in a single platform.

## What plans are available?

NovaCRM offers three plans:

- **Free**: Up to 500 contacts, 2 users, basic pipeline. No credit card required.
- **Pro** ($29/user/month): Unlimited contacts, custom fields, workflow automation, API access (10,000 requests/day), email integration, and priority support.
- **Enterprise** ($79/user/month): Everything in Pro plus SSO/SAML, audit logs, custom roles, dedicated account manager, API access (100,000 requests/day), and 99.9% SLA.

All plans include a 14-day free trial of Pro features.

## What are the API rate limits?

API rate limits depend on your plan:

- Free: 1,000 requests per day, 10 requests per second
- Pro: 10,000 requests per day, 50 requests per second
- Enterprise: 100,000 requests per day, 200 requests per second

Rate limit headers are included in every response: `X-RateLimit-Remaining` and `X-RateLimit-Reset`.

## What integrations are supported?

NovaCRM integrates with Slack, Salesforce, HubSpot, Stripe, Zendesk, Jira, Google Workspace, and Microsoft 365. Custom integrations are available via webhooks and our REST API. Zapier and Make (Integromat) connectors are also available.

## How is data secured?

All data is encrypted at rest (AES-256) and in transit (TLS 1.3). We are SOC 2 Type II certified and GDPR compliant. Enterprise plans include SSO with SAML 2.0 and SCIM provisioning. Audit logs track all user actions for 12 months.

## Can I export my data?

Yes. All plans support CSV export of contacts, deals, and activities. Pro and Enterprise plans also support JSON export via the API. Full database exports are available for Enterprise customers upon request.
