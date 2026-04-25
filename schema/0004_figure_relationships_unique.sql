-- Audit D4: figure_relationships has 74 duplicate (a,b,relationship)
-- triples and no UNIQUE constraint or self-referential CHECK.
-- Dedupe (keeping the lowest id) before adding the constraint.

BEGIN;

DELETE FROM figure_relationships a
USING figure_relationships b
WHERE a.id > b.id
  AND a.figure_a_id  = b.figure_a_id
  AND a.figure_b_id  = b.figure_b_id
  AND a.relationship = b.relationship;

ALTER TABLE figure_relationships
    ADD CONSTRAINT figure_relationships_unique
    UNIQUE (figure_a_id, figure_b_id, relationship);

ALTER TABLE figure_relationships
    ADD CONSTRAINT figure_relationships_not_self
    CHECK (figure_a_id <> figure_b_id);

COMMIT;
