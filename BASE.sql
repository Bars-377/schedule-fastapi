CREATE TABLE "branches" (
	"id" SERIAL NOT NULL,
	"name" VARCHAR(255) NULL DEFAULT NULL::character varying,
	"department_id" INTEGER NULL DEFAULT NULL,
	PRIMARY KEY ("id")
)
;
COMMENT ON COLUMN "branches"."id" IS '';
COMMENT ON COLUMN "branches"."name" IS '';
COMMENT ON COLUMN "branches"."department_id" IS '';

CREATE TABLE "branchdata" (
	"id" SERIAL NOT NULL,
	"branch_id" INTEGER NULL DEFAULT NULL,
	"metric_id" INTEGER NULL DEFAULT NULL,
	"record_date" DATE NULL DEFAULT NULL,
	"value" NUMERIC(12,2) NULL DEFAULT NULL::numeric,
	PRIMARY KEY ("id"),
	CONSTRAINT branch_metric_date_unique UNIQUE (branch_id, metric_id, record_date)
)
;
COMMENT ON COLUMN "branchdata"."id" IS '';
COMMENT ON COLUMN "branchdata"."branch_id" IS '';
COMMENT ON COLUMN "branchdata"."metric_id" IS '';
COMMENT ON COLUMN "branchdata"."record_date" IS '';
COMMENT ON COLUMN "branchdata"."value" IS '';

CREATE TABLE "metrics" (
	"id" SERIAL NOT NULL,
	"name" VARCHAR(255) NULL DEFAULT NULL::character varying,
	PRIMARY KEY ("id")
)
;
COMMENT ON COLUMN "metrics"."id" IS '';
COMMENT ON COLUMN "metrics"."name" IS '';

CREATE TABLE "users" (
	"id" SERIAL NOT NULL,
	"username" VARCHAR(255) NULL DEFAULT NULL::character varying,
	"hashed_password" VARCHAR(255) NULL DEFAULT NULL::character varying,
	"can_edit" INTEGER NULL DEFAULT NULL,
	PRIMARY KEY ("id")
)
;
COMMENT ON COLUMN "users"."id" IS '';
COMMENT ON COLUMN "users"."username" IS '';
COMMENT ON COLUMN "users"."hashed_password" IS '';
COMMENT ON COLUMN "users"."can_edit" IS '';
