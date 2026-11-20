CREATE TABLE deliveries (
    id uuid PRIMARY KEY,
    queue_id bigint UNIQUE,
    event_id varchar(128) NOT NULL,
    destination_url text NOT NULL,
    domain text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued'
      CHECK (status IN ('queued','sending','retrying','success','failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 4),
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX deliveries_cleanup ON deliveries(updated_at)
    WHERE status IN ('success','failed');
CREATE TABLE delivery_attempts (
    delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    number integer NOT NULL CHECK (number BETWEEN 1 AND 4),
    status text NOT NULL CHECK (status IN ('sending','success','failed','retrying','unknown')),
    response_code integer,
    error text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    duration_ms integer,
    retry_delay_seconds double precision,
    PRIMARY KEY (delivery_id, number)
);
CREATE TABLE domain_buckets (
    domain text PRIMARY KEY,
    tokens double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
