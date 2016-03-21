SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM "schema" WHERE "version" = '4') THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END
$$;

ALTER TABLE "counter" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "lock" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "hash" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "job" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "jobparameter" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "jobqueue" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "list" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "server" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "set" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "state" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;

DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_job_expireat" ON "job" ("expireat");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_job_expireat" already exists.';
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_list_expireat" ON "list" ("expireat");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_list_expireat" already exists.';
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_set_expireat" ON "set" ("expireat");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_set_expireat" already exists.';
    END;
END;
$$;