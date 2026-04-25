-- Audit D2: documents.source has no btree index. EXPLAIN on filter
-- queries shows seq scan over 141K rows. CONCURRENTLY to avoid taking
-- an ACCESS EXCLUSIVE lock on a hot table.
--
-- Run outside of a transaction:
--   psql $ISLAM_STORIES_DB_URL -f schema/0002_index_documents_source.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_documents_source
    ON documents USING btree (source);
