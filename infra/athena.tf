# Athena query layer over the curated and gold zones: a Glue database, the tables the dashboard reads,
# a workgroup with a scan limit, and the two views. Replaces the hand-run dashboard/athena/*.sql setup.
# Tables hold no data; Athena reads the files in S3. `date` is projected, so there is no crawler and no
# MSCK REPAIR, and a new day's folder is queryable as soon as it exists.

locals {
  athena_database  = replace(var.project, "-", "_")
  athena_workgroup = var.project

  curated_location = "s3://${aws_s3_bucket.curated.id}/${var.curated_prefix}"
  gold_bucket_uri  = "s3://${aws_s3_bucket.scoring["gold"].id}"
  queue_location   = "${local.gold_bucket_uri}/bq1/queue/${var.athena_queue_run}"
  sharing_location = "${local.gold_bucket_uri}/${var.curated_prefix}/sharing"

  # Column names and types follow prepare/prepare.py and the dashboard/athena SQL.
  transfer_columns = {
    timestamp               = "timestamp"
    sender_bank_id          = "string"
    sender_account_number   = "string"
    receiver_bank_id        = "string"
    receiver_account_number = "string"
    amount_received         = "double"
    receiving_currency      = "string"
    amount_paid             = "double"
    payment_currency        = "string"
    payment_format          = "string"
    transfer_id             = "string"
    source_file             = "string"
    source_sha256           = "string"
    source_row_id           = "bigint"
  }
  transfer_column_order = ["timestamp", "sender_bank_id", "sender_account_number", "receiver_bank_id",
    "receiver_account_number", "amount_received", "receiving_currency", "amount_paid", "payment_currency",
  "payment_format", "transfer_id", "source_file", "source_sha256", "source_row_id"]

  label_columns = [
    ["transfer_id", "string"], ["source_file", "string"], ["source_sha256", "string"],
    ["source_row_id", "bigint"], ["laundering_mark", "int"],
  ]
  ring_columns = [
    ["ring_id", "bigint"], ["pattern_type", "string"], ["timestamp", "timestamp"],
    ["sender_bank_id", "string"], ["sender_account_number", "string"], ["receiver_bank_id", "string"],
    ["receiver_account_number", "string"], ["amount_received", "double"], ["receiving_currency", "string"],
    ["amount_paid", "double"], ["payment_currency", "string"], ["payment_format", "string"],
    ["laundering_mark", "int"], ["date", "date"],
  ]
  queue_columns = [
    ["rank", "string"], ["bank_id", "string"], ["account_id", "string"], ["score", "string"],
    ["model_version", "string"], ["window_start", "string"], ["cutoff", "string"],
    ["reason_1", "string"], ["reason_2", "string"], ["reason_3", "string"],
  ]
  sharing_columns = [
    ["bank_id", "string"], ["view", "string"], ["k", "int"], ["reviewed", "int"], ["eligible", "int"],
    ["positive_accounts", "int"], ["true_positives", "int"], ["precision_at_k", "double"],
    ["recall_at_k", "double"], ["model", "string"],
  ]

  # Projects the `date` partition over the dataset's days, so partitions never need registering.
  date_projection = {
    "projection.enabled"            = "true"
    "projection.date.type"          = "date"
    "projection.date.range"         = var.athena_date_range
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"
  }

  parquet = {
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }
  text_input  = "org.apache.hadoop.mapred.TextInputFormat"
  text_output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

  views_sql = "${path.module}/../dashboard/athena/05_views.sql"
}

resource "aws_glue_catalog_database" "this" {
  name        = local.athena_database
  description = "Team 6 AML pipeline outputs, queried through Athena."
}

resource "aws_athena_workgroup" "this" {
  name          = local.athena_workgroup
  description   = "Team 6 queries, ${var.athena_scan_limit_gib} GB scan limit per query"
  force_destroy = var.force_destroy_buckets

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_gib * 1024 * 1024 * 1024

    result_configuration {
      output_location = "${local.gold_bucket_uri}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Parquet, one folder per day, written by Prepare.
resource "aws_glue_catalog_table" "transfers" {
  name          = "transfers"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters = merge(local.date_projection, {
    EXTERNAL                    = "TRUE"
    "storage.location.template" = "${local.curated_location}/transfers/date=$${date}/"
  })

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.curated_location}/transfers/"
    input_format  = local.parquet.input_format
    output_format = local.parquet.output_format
    ser_de_info {
      serialization_library = local.parquet.serde
    }
    dynamic "columns" {
      for_each = local.transfer_column_order
      content {
        name = columns.value
        type = local.transfer_columns[columns.value]
      }
    }
  }
}

# Ground truth, kept apart from the transfers on purpose (ADR-0002). Join on transfer_id.
resource "aws_glue_catalog_table" "transfer_labels" {
  name          = "transfer_labels"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters = merge(local.date_projection, {
    EXTERNAL                    = "TRUE"
    "storage.location.template" = "${local.curated_location}/transfer_labels/date=$${date}/"
  })

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.curated_location}/transfer_labels/"
    input_format  = local.parquet.input_format
    output_format = local.parquet.output_format
    ser_de_info {
      serialization_library = local.parquet.serde
    }
    dynamic "columns" {
      for_each = local.label_columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

# One row per transfer in the patterns file. Not partitioned: `date` is an ordinary column here.
resource "aws_glue_catalog_table" "rings" {
  name          = "rings"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "${local.curated_location}/rings/"
    input_format  = local.parquet.input_format
    output_format = local.parquet.output_format
    ser_de_info {
      serialization_library = local.parquet.serde
    }
    dynamic "columns" {
      for_each = local.ring_columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

# BQ1 review queue. Each day's folder holds accounts.csv, ranking.json and _SUCCESS; the queue_accounts
# view keeps accounts.csv only. Every column is text because the CSV is quoted where a reason has a comma.
resource "aws_glue_catalog_table" "queue_files" {
  name          = "queue_files"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters = merge(local.date_projection, {
    EXTERNAL                    = "TRUE"
    "skip.header.line.count"    = "1"
    "storage.location.template" = "${local.queue_location}/date=$${date}/"
  })

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.queue_location}/"
    input_format  = local.text_input
    output_format = local.text_output
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        separatorChar          = ","
        quoteChar              = "\""
        escapeChar             = "\\"
        "serialization.format" = "1"
      }
    }
    dynamic "columns" {
      for_each = local.queue_columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }

  # Each pipeline run re-points this table at its own queue (PointQueueTable in the state machine), so
  # athena_queue_run only sets where it starts. Without this, every apply would point it back.
  lifecycle {
    ignore_changes = [
      storage_descriptor[0].location,
      parameters["storage.location.template"],
      parameters["transient_lastDdlTime"],
    ]
  }
}

# BQ2 results. The folder holds comparison.csv and comparison_by_day.csv; the sharing_comparison view
# keeps comparison.csv.
resource "aws_glue_catalog_table" "sharing_files" {
  name          = "sharing_files"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL                 = "TRUE"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "${local.sharing_location}/"
    input_format  = local.text_input
    output_format = local.text_output
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = ",", "serialization.format" = "," }
    }
    dynamic "columns" {
      for_each = local.sharing_columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

# Glue views are Presto-encoded JSON, which Terraform cannot write cleanly, so the two views are created
# through Athena DDL. This runs where `terraform apply` runs: it needs python3, boto3 and AWS credentials.
# It reruns when the SQL changes or a table behind the views is replaced.
resource "terraform_data" "views" {
  count = var.athena_create_views ? 1 : 0

  triggers_replace = [
    filemd5(local.views_sql),
    aws_glue_catalog_table.queue_files.id,
    aws_glue_catalog_table.sharing_files.id,
    aws_athena_workgroup.this.id,
  ]

  provisioner "local-exec" {
    command = "python3 ${path.module}/../dashboard/athena/run_sql.py ${local.views_sql} --workgroup ${local.athena_workgroup} --database ${local.athena_database} --region ${var.region}"
  }
}

# After ScoreBQ1, point queue_files at the queue this execution just wrote, so the dashboard shows
# the newest run with nobody editing SQL or tfvars. Two DDL statements: Athena runs one per query.
# In States.Format, { } and ' are escaped with a backslash.
locals {
  athena_query_settings = {
    WorkGroup             = local.athena_workgroup
    QueryExecutionContext = { Database = local.athena_database }
  }
  athena_retry = [{
    ErrorEquals     = ["Athena.TooManyRequestsException", "Athena.InternalServerException"]
    IntervalSeconds = 10
    MaxAttempts     = 3
    BackoffRate     = 2
  }]

  athena_states = {
    PointQueueTable = {
      Type     = "Task"
      Resource = "arn:aws:states:::athena:startQueryExecution.sync"
      Parameters = merge(local.athena_query_settings, {
        "QueryString.$" = "States.Format('ALTER TABLE queue_files SET LOCATION \\'${local.bq1_queue_uri}/{}/\\'', $$.Execution.Name)"
      })
      TimeoutSeconds = 300
      Retry          = local.athena_retry
      ResultPath     = null
      Next           = "PointQueueTemplate"
    }

    PointQueueTemplate = {
      Type     = "Task"
      Resource = "arn:aws:states:::athena:startQueryExecution.sync"
      Parameters = merge(local.athena_query_settings, {
        "QueryString.$" = "States.Format('ALTER TABLE queue_files SET TBLPROPERTIES (\\'storage.location.template\\' = \\'${local.bq1_queue_uri}/{}/date=$\\{date\\}/\\')', $$.Execution.Name)"
      })
      TimeoutSeconds = 300
      Retry          = local.athena_retry
      ResultPath     = null
      Next           = "ScoringConfiguration"
    }
  }
}

# What the dashboard's users need. Attach it to the team's IAM group or users.
data "aws_iam_policy_document" "athena_query" {
  statement {
    sid       = "Athena"
    actions   = ["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"]
    resources = [aws_athena_workgroup.this.arn]
  }
  statement {
    sid     = "GlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"]
    resources = [
      "arn:aws:glue:${var.region}:${local.account_id}:catalog",
      "arn:aws:glue:${var.region}:${local.account_id}:database/${local.athena_database}",
      "arn:aws:glue:${var.region}:${local.account_id}:table/${local.athena_database}/*",
    ]
  }
  statement {
    sid     = "ReadData"
    actions = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/*",
      aws_s3_bucket.scoring["gold"].arn, "${aws_s3_bucket.scoring["gold"].arn}/*",
    ]
  }
  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.scoring["gold"].arn}/athena-results/*"]
  }
}

resource "aws_iam_policy" "athena_query" {
  name        = "${var.project}-athena-query"
  description = "Run Athena queries in the ${local.athena_workgroup} workgroup and read the curated and gold zones."
  policy      = data.aws_iam_policy_document.athena_query.json
}
