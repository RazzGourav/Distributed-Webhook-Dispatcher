"""Exercise a deployed API without printing credentials or payloads."""

import argparse
import os
import time

import httpx
from dotenv import load_dotenv


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--receiver-base", required=True)
    args = parser.parse_args()
    key = os.environ.get("API_KEY")
    if not key:
        raise SystemExit("Set API_KEY in your environment or local .env")
    base = args.base_url.rstrip("/")
    receiver = args.receiver_base.rstrip("/")
    # Public warm-up, to avoid misreporting a platform cold start as API blocking.
    with httpx.Client(timeout=120, trust_env=False) as warmup:
        warmup.get(f"{base}/health/ready").raise_for_status()
    with httpx.Client(
        base_url=base, headers={"X-API-Key": key}, timeout=15, trust_env=False
    ) as client:
        for mode, expected_status, expected_attempts in [
            ("success", "success", 1),
            ("flaky", "success", 3),
            ("fail", "failed", 4),
            ("slow", "success", 1),
        ]:
            started = time.monotonic()
            response = client.post(
                "/api/webhooks/dispatch",
                json={
                    "destinationUrl": f"{receiver}/demo/receiver/{mode}",
                    "eventId": f"smoke_{mode}_{time.time_ns()}",
                    "payload": {"test": mode},
                },
            )
            elapsed = time.monotonic() - started
            assert response.status_code == 202, f"{mode}: ingestion HTTP {response.status_code}"
            identifier = response.json()["jobId"]
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                result = client.get(f"/api/webhooks/jobs/{identifier}")
                result.raise_for_status()
                job = result.json()
                if job["status"] in {"success", "failed"}:
                    break
                time.sleep(0.5)
            else:
                raise AssertionError(f"{mode}: delivery did not finish")
            assert job["status"] == expected_status, f"{mode}: unexpected {job}"
            assert job["attemptCount"] == expected_attempts
            if mode == "slow":
                assert elapsed < 3, f"Warm ingestion unexpectedly slow: {elapsed:.3f}s"
            print(
                f"PASS {mode}: ingestion={elapsed:.3f}s status={job['status']} attempts={job['attemptCount']}"
            )


if __name__ == "__main__":
    main()
