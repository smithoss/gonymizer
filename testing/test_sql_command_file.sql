BEGIN;
  SELECT count(*) FROM information_schema.tables;
ROLLBACK;
