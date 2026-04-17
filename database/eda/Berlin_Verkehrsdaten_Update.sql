CREATE SCHEMA "ingestion";

CREATE SCHEMA "raw";

CREATE TABLE "stg_stammdaten_mq" (
  "mq_id15" bigint,
  "mq_kurzname" varchar,
  "strasse" varchar,
  "position" varchar,
  "pos_detail" varchar,
  "richtung" varchar,
  "lon_wgs84" numeric(10,6),
  "lat_wgs84" numeric(10,6),
  "snapshot_date" date,
  "source_file" varchar,
  "ingested_at" timestamptz
);

CREATE TABLE "stg_stammdaten_detector" (
  "det_id15" bigint,
  "mq_id15" bigint,
  "det_name_alt" varchar,
  "det_name_neu" varchar,
  "spur" varchar,
  "annotation" varchar,
  "kommentar" varchar,
  "inbetriebnahme" date,
  "abbaudatum" date,
  "deinstalliert" varchar,
  "lon_wgs84" numeric(10,6),
  "lat_wgs84" numeric(10,6),
  "snapshot_date" date,
  "source_file" varchar,
  "ingested_at" timestamptz
);

CREATE TABLE "stg_old_det_val_hr" (
  "detid_15" bigint,
  "tag" date,
  "stunde" int,
  "qualitaet" decimal(5,2),
  "q_kfz_det_hr" int,
  "v_kfz_det_hr" int,
  "q_pkw_det_hr" int,
  "v_pkw_det_hr" int,
  "q_lkw_det_hr" int,
  "v_lkw_det_hr" int,
  "source_file" varchar,
  "ingested_at" timestamptz
);

CREATE TABLE "stg_old_mq_hr" (
  "mq_name" varchar,
  "tag" date,
  "stunde" int,
  "qualitaet" decimal(5,2),
  "q_kfz_mq_hr" int,
  "v_kfz_mq_hr" int,
  "q_pkw_mq_hr" int,
  "v_pkw_mq_hr" int,
  "q_lkw_mq_hr" int,
  "v_lkw_mq_hr" int,
  "source_file" varchar,
  "ingested_at" timestamptz
);

CREATE TABLE "stg_new_detector_hourly" (
  "det_name_alt" varchar,
  "det_index" int,
  "datum_ortszeit" date,
  "stunde_ortszeit" int,
  "vollstaendigkeit" numeric(12,6),
  "zscore_det0" numeric(12,6),
  "zscore_det1" numeric(12,6),
  "zscore_det2" numeric(12,6),
  "hist_cor" numeric(12,6),
  "local_time" timestamptz,
  "month" int,
  "qkfz" int,
  "qlkw" int,
  "qpkw" int,
  "utc" timestamptz,
  "vkfz" int,
  "vlkw" int,
  "vpkw" int,
  "source_file" varchar,
  "ingested_at" timestamptz
);

CREATE TABLE "dim_mq" (
  "mq_id15" bigint PRIMARY KEY,
  "mq_kurzname" varchar UNIQUE,
  "strasse" varchar,
  "position" varchar,
  "pos_detail" varchar,
  "richtung" varchar,
  "lon_wgs84" numeric(10,6),
  "lat_wgs84" numeric(10,6)
);

CREATE TABLE "dim_detector" (
  "det_id15" bigint PRIMARY KEY,
  "mq_id15" bigint,
  "det_name_alt" varchar UNIQUE,
  "det_name_neu" varchar,
  "spur" varchar,
  "annotation" varchar,
  "kommentar" varchar,
  "inbetriebnahme" date,
  "abbaudatum" date,
  "deinstalliert" varchar,
  "lon_wgs84" numeric(10,6),
  "lat_wgs84" numeric(10,6)
);

CREATE TABLE "dim_vehicle_class" (
  "vehicle_class_id" smallint PRIMARY KEY,
  "code" varchar UNIQUE,
  "label" varchar
);

CREATE TABLE "dim_time_hour" (
  "ts_utc" timestamptz PRIMARY KEY,
  "date_local" date,
  "hour_local" int,
  "month_local" int
);

CREATE TABLE "fact_detector_hourly" (
  "det_id15" bigint,
  "ts_utc" timestamptz,
  "vehicle_class_id" smallint,
  "flow_q" int,
  "speed_v" int,
  "source_layout" varchar,
  "created_at" timestamptz
);

CREATE TABLE "fact_mq_hourly" (
  "mq_id15" bigint,
  "ts_utc" timestamptz,
  "vehicle_class_id" smallint,
  "flow_q" int,
  "speed_v" int,
  "source_layout" varchar,
  "created_at" timestamptz
);

CREATE TABLE "fact_quality_hourly" (
  "det_id15" bigint,
  "ts_utc" timestamptz,
  "quality_old" numeric(12,6),
  "completeness_new" numeric(12,6),
  "zscore_det0" numeric(12,6),
  "zscore_det1" numeric(12,6),
  "zscore_det2" numeric(12,6),
  "hist_cor" numeric(12,6),
  "source_layout" varchar,
  "created_at" timestamptz
);

CREATE TABLE "pipeline_run" (
  "run_id" uuid PRIMARY KEY,
  "started_at" timestamptz,
  "finished_at" timestamptz,
  "status" varchar,
  "source_year" int,
  "source_month" int,
  "source_layout" varchar,
  "notes" varchar
);

CREATE TABLE "qa_features_hourly" (
  "det_id15" bigint,
  "ts_utc" timestamptz,
  "run_id" uuid,
  "row_count" int,
  "missing_rate" numeric(18,6),
  "duplicate_rate" numeric(18,6),
  "freshness_lag_h" int
);

CREATE TABLE "ml_anomaly_score_hourly" (
  "det_id15" bigint,
  "ts_utc" timestamptz,
  "run_id" uuid,
  "model_name" varchar,
  "anomaly_score" numeric(12,6),
  "is_anomaly" boolean,
  "top_driver" varchar,
  "driver_value" numeric(12,6)
);

CREATE TABLE "kpi_definition" (
  "kpi_id" INT PRIMARY KEY,
  "kpi_name" varchar UNIQUE,
  "description" text,
  "grain" varchar,
  "owner" varchar,
  "formula" text,
  "is_active" boolean,
  "version" int,
  "created_at" timestamptz
);

CREATE TABLE "kpi_value" (
  "kpi_id" int,
  "ts_utc" timestamptz,
  "entity_type" varchar,
  "entity_id" bigint,
  "value" numeric(18,6),
  "run_id" uuid,
  "calculated_at" timestamptz
);

CREATE TABLE "kpi_confidence" (
  "kpi_id" int,
  "ts_utc" timestamptz,
  "entity_type" varchar,
  "entity_id" bigint,
  "confidence_score" numeric(12,6),
  "confidence_label" varchar,
  "freshness_score" numeric(12,6),
  "volume_score" numeric(12,6),
  "null_score" numeric(12,6),
  "anomaly_score" numeric(12,6),
  "drift_score" numeric(12,6),
  "run_id" uuid,
  "calculated_at" timestamptz
);

CREATE TABLE "ingestion"."ingestion_runs" (
  "run_id" uuid PRIMARY KEY,
  "triggered_by" varchar,
  "status" varchar,
  "started_at" timestamptz,
  "ended_at" timestamptz,
  "notes" text,
  "payload" jsonb
);

CREATE TABLE "ingestion"."file_manifest" (
  "source_url" text PRIMARY KEY,
  "dataset_version" varchar,
  "source_type" varchar,
  "month_key" varchar,
  "checksum_sha256" varchar,
  "bytes" bigint,
  "last_modified" timestamptz,
  "first_seen_at" timestamptz,
  "last_seen_at" timestamptz,
  "last_ingested_at" timestamptz,
  "last_ingestion_run_id" uuid,
  "last_status" varchar,
  "metadata" jsonb
);

CREATE TABLE "ingestion"."run_files" (
  "run_id" uuid,
  "source_url" text,
  "action" varchar,
  "row_count" bigint,
  "message" text,
  "created_at" timestamptz,
  PRIMARY KEY ("run_id", "source_url")
);

CREATE TABLE "raw"."traffic_rows" (
  "id" bigint PRIMARY KEY,
  "run_id" uuid,
  "source_url" text,
  "month_key" varchar,
  "dataset_version" varchar,
  "source_type" varchar,
  "row_number" bigint,
  "payload" jsonb,
  "loaded_at" timestamptz
);

ALTER TABLE "dim_detector" ADD FOREIGN KEY ("mq_id15") REFERENCES "dim_mq" ("mq_id15");

ALTER TABLE "fact_detector_hourly" ADD FOREIGN KEY ("det_id15") REFERENCES "dim_detector" ("det_id15");

ALTER TABLE "fact_detector_hourly" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "fact_detector_hourly" ADD FOREIGN KEY ("vehicle_class_id") REFERENCES "dim_vehicle_class" ("vehicle_class_id");

ALTER TABLE "fact_mq_hourly" ADD FOREIGN KEY ("mq_id15") REFERENCES "dim_mq" ("mq_id15");

ALTER TABLE "fact_mq_hourly" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "fact_mq_hourly" ADD FOREIGN KEY ("vehicle_class_id") REFERENCES "dim_vehicle_class" ("vehicle_class_id");

ALTER TABLE "fact_quality_hourly" ADD FOREIGN KEY ("det_id15") REFERENCES "dim_detector" ("det_id15");

ALTER TABLE "fact_quality_hourly" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "qa_features_hourly" ADD FOREIGN KEY ("det_id15") REFERENCES "dim_detector" ("det_id15");

ALTER TABLE "qa_features_hourly" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "qa_features_hourly" ADD FOREIGN KEY ("run_id") REFERENCES "pipeline_run" ("run_id");

ALTER TABLE "ml_anomaly_score_hourly" ADD FOREIGN KEY ("det_id15") REFERENCES "dim_detector" ("det_id15");

ALTER TABLE "ml_anomaly_score_hourly" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "ml_anomaly_score_hourly" ADD FOREIGN KEY ("run_id") REFERENCES "pipeline_run" ("run_id");

ALTER TABLE "kpi_value" ADD FOREIGN KEY ("kpi_id") REFERENCES "kpi_definition" ("kpi_id");

ALTER TABLE "kpi_value" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "kpi_value" ADD FOREIGN KEY ("run_id") REFERENCES "pipeline_run" ("run_id");

ALTER TABLE "kpi_confidence" ADD FOREIGN KEY ("kpi_id") REFERENCES "kpi_definition" ("kpi_id");

ALTER TABLE "kpi_confidence" ADD FOREIGN KEY ("ts_utc") REFERENCES "dim_time_hour" ("ts_utc");

ALTER TABLE "kpi_confidence" ADD FOREIGN KEY ("run_id") REFERENCES "pipeline_run" ("run_id");

ALTER TABLE "ingestion"."run_files" ADD FOREIGN KEY ("run_id") REFERENCES "ingestion"."ingestion_runs" ("run_id");

ALTER TABLE "ingestion"."run_files" ADD FOREIGN KEY ("source_url") REFERENCES "ingestion"."file_manifest" ("source_url");

ALTER TABLE "raw"."traffic_rows" ADD FOREIGN KEY ("run_id") REFERENCES "ingestion"."ingestion_runs" ("run_id");

ALTER TABLE "raw"."traffic_rows" ADD FOREIGN KEY ("source_url") REFERENCES "ingestion"."file_manifest" ("source_url");
