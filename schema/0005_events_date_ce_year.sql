-- Audit D5: events.date_ce is text ("839", "622", "632-633", "-539").
-- Add an integer date_ce_year for cheap ordering/filtering.
-- Verified: of all populated date_ce, only 4 rows are non-numeric
-- (-539, -546, 632-633, 685-687). The regex grabs the first 3-4 digit
-- run, so "632-633" → 632, "-539" → 539 (negative sign dropped — BCE
-- dates are rare; spot-fix manually after migration).

BEGIN;

ALTER TABLE events ADD COLUMN IF NOT EXISTS date_ce_year INTEGER;

UPDATE events
SET    date_ce_year = (regexp_match(date_ce, '\d{3,4}'))[1]::int
WHERE  date_ce ~ '\d{3,4}'
  AND  date_ce_year IS NULL;

CREATE INDEX IF NOT EXISTS idx_events_date_ce_year
    ON events (date_ce_year);

COMMIT;

-- After running, spot-check rows still NULL:
--   SELECT id, name, date_ce FROM events
--   WHERE date_ce IS NOT NULL AND date_ce_year IS NULL;
