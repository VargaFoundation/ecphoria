# NovaCRM — Troubleshooting Guide

## Login Issues

**Problem**: "Invalid credentials" error after password reset.
**Solution**: Clear your browser cache and cookies, then try logging in again. Password resets can take up to 60 seconds to propagate. If using SSO, ensure your identity provider session is active.

**Problem**: Two-factor authentication code not accepted.
**Solution**: Check that your device clock is synchronized (TOTP codes are time-sensitive). If your authenticator app was reinstalled, use one of your backup codes and re-enroll 2FA from Settings > Security.

## Sync Problems

**Problem**: Emails not appearing in contact timeline.
**Solution**: Verify your email integration is connected under Settings > Integrations > Email. Check that the contact's email address matches exactly. Gmail users: ensure "less secure app access" or an app-specific password is configured.

**Problem**: Salesforce sync showing duplicate contacts.
**Solution**: Enable deduplication rules under Settings > Integrations > Salesforce > Advanced. Set the merge strategy to "match by email" and run a manual sync to resolve existing duplicates.

## Performance Issues

**Problem**: Dashboard loading slowly (>10 seconds).
**Solution**: Reduce the date range in your dashboard filters. Dashboards covering more than 90 days of data may be slow on large accounts. Consider creating saved views with specific date ranges. Enterprise customers can contact support to enable dashboard caching.

**Problem**: API responses timing out.
**Solution**: Check your query parameters — requests returning more than 1,000 records should use pagination (add `?page=1&per_page=100`). Bulk operations should use the `/api/v1/bulk` endpoint instead of individual requests.

## Webhook Issues

**Problem**: Webhooks not firing for new events.
**Solution**: Check the webhook URL is accessible from the internet (no localhost or private IPs). Verify the webhook is enabled under Settings > Webhooks. Check the delivery log for HTTP status codes — we retry failed deliveries 3 times with exponential backoff.

## Billing

**Problem**: Charged after cancellation.
**Solution**: Cancellations take effect at the end of the billing period. If you were charged after canceling, check your cancellation confirmation email for the effective date. For immediate refunds, contact billing@novacrm.example.com.
