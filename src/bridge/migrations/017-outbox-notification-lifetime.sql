ALTER TABLE outbox ADD COLUMN expires_at_ms INTEGER CHECK (expires_at_ms >= 0);
ALTER TABLE outbox ADD COLUMN discarded_at_ms INTEGER
  CHECK (discarded_at_ms >= 0 AND (discarded_at_ms IS NULL OR body IS NULL));

CREATE INDEX outbox_discarded_retention
  ON outbox (discarded_at_ms)
  WHERE discarded_at_ms IS NOT NULL;
