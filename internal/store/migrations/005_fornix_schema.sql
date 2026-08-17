-- Move the internal namespace to the product name after legacy migrations
-- have been applied. This preserves existing databases while removing the
-- compatibility namespace from all current runtime queries.
DO $$
BEGIN
  IF to_regclass('fabric.schema_migrations') IS NOT NULL
     AND to_regclass('fornix.schema_migrations') IS NULL THEN
    ALTER SCHEMA fabric RENAME TO fornix;
  END IF;
END $$;
