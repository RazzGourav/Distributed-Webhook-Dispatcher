# Submission preparation — deadline 22 November 2026

Do not submit until the public deployment passes the smoke check. The deadline
notice does not specify a time, so leave time for deployment and reviewer access.

## Final verification

- GitHub repository has source, README, lockfile, tests and Render configuration.
- CI passes on the exact commit being deployed.
- Public `/health/ready` returns 200.
- A fresh private evaluation key works in Swagger and curl.
- Live smoke scenarios pass: success, flaky, fail, slow.
- Document the actual URL and database expiry date in your private submission.
- Share the API key only with the evaluator, not in README or Git history.

## Email draft

To: hr@web3task.com

Subject: Backend Development Intern Evaluation — Distributed Webhook Dispatcher — Gourav

Dear Hiring Team,

Thank you for the opportunity to complete the Backend Development Intern evaluation.

I have implemented the Distributed Webhook Dispatcher using Python, FastAPI and
PostgreSQL, with persistent background processing through Procrastinate.

Repository: https://github.com/RazzGourav/Distributed-Webhook-Dispatcher

Live API: [INSERT VERIFIED PUBLIC URL]

Interactive documentation: [LIVE URL]/docs

Evaluation API key: [INSERT PRIVATELY; SEND AS X-API-Key]

The implementation includes non-blocking ingestion, exponential retry delays,
shared per-domain rate limiting, delivery-attempt history, automated tests and
reproducible setup instructions. The README includes example requests, demo
receiver scenarios, architecture decisions and hosting limitations.

The free hosting instance may need a cold start before the first request. Once
awake, the dispatch endpoint queues work without waiting for downstream delivery.

I look forward to discussing the implementation and design choices.

Regards,
Gourav

---

This is a draft only. Replace the placeholders and verify deployment before sending.
