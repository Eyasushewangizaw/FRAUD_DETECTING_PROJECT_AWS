# Amazon Redshift provisioned cluster (single-node dev shape), COPY IAM role, and optional DDL bootstrap via Data API.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnet" "default_detail" {
  for_each = toset(data.aws_subnets.default.ids)
  id       = each.value
}

locals {
  subnets_by_az = {
    for id, sub in data.aws_subnet.default_detail :
    sub.availability_zone => id
  }
  ordered_azs = sort(keys(local.subnets_by_az))
  redshift_subnet_ids = length(local.ordered_azs) >= 2 ? [
    local.subnets_by_az[local.ordered_azs[0]],
    local.subnets_by_az[local.ordered_azs[1]],
  ] : []
}

resource "aws_redshift_subnet_group" "this" {
  name       = "${var.project_name}-subnet-group"
  subnet_ids = local.redshift_subnet_ids

  tags = local.common_tags

  lifecycle {
    precondition {
      condition     = length(local.redshift_subnet_ids) == 2
      error_message = "The default VPC must expose a default subnet in at least two Availability Zones so Redshift can build a valid subnet group."
    }
  }
}

resource "aws_security_group" "redshift" {
  name_prefix = "${var.project_name}-rs-"
  vpc_id      = data.aws_vpc.default.id
  description = "Ingress to the fraud Redshift cluster (restrict CIDRs in production)."

  ingress {
    description = "Redshift"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = var.redshift_allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = local.common_tags
}

resource "random_password" "redshift_master" {
  length  = 20
  special = false
}

resource "aws_secretsmanager_secret" "redshift_master" {
  name_prefix             = "${var.project_name}-rs-master-"
  recovery_window_in_days = 0

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "redshift_master" {
  secret_id = aws_secretsmanager_secret.redshift_master.id
  secret_string = jsonencode({
    username = var.redshift_master_username
    password = random_password.redshift_master.result
  })
}

resource "aws_redshift_cluster" "this" {
  cluster_identifier                  = "${var.project_name}-warehouse"
  database_name                       = "dev"
  master_username                     = var.redshift_master_username
  master_password                     = random_password.redshift_master.result
  node_type                           = var.redshift_node_type
  cluster_type                        = "single-node"
  publicly_accessible                 = true
  skip_final_snapshot                 = true
  automated_snapshot_retention_period = 1
  encrypted                           = true

  cluster_subnet_group_name    = aws_redshift_subnet_group.this.name
  vpc_security_group_ids       = [aws_security_group.redshift.id]
  default_iam_role_arn         = aws_iam_role.redshift_lake_read.arn
  allow_version_upgrade        = true
  preferred_maintenance_window = "sun:05:00-sun:06:00"

  tags = local.common_tags

  depends_on = [aws_secretsmanager_secret_version.redshift_master]
}

resource "time_sleep" "redshift_bootstrap_delay" {
  count           = var.redshift_apply_bootstrap_sql ? 1 : 0
  depends_on      = [aws_redshift_cluster.this]
  create_duration = "90s"
}

resource "aws_redshiftdata_statement" "bootstrap_schema" {
  count = var.redshift_apply_bootstrap_sql ? 1 : 0

  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  database           = aws_redshift_cluster.this.database_name
  secret_arn         = aws_secretsmanager_secret.redshift_master.arn
  sql                = "CREATE SCHEMA IF NOT EXISTS fraud;"

  depends_on = [time_sleep.redshift_bootstrap_delay[0]]
}

resource "aws_redshiftdata_statement" "bootstrap_fact" {
  count = var.redshift_apply_bootstrap_sql ? 1 : 0

  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  database           = aws_redshift_cluster.this.database_name
  secret_arn         = aws_secretsmanager_secret.redshift_master.arn
  sql                = <<-SQL
    CREATE TABLE IF NOT EXISTS fraud.fact_transactions (
        transaction_id          VARCHAR(50) NOT NULL,
        card_number_hash        VARCHAR(64),
        merchant_id             VARCHAR(50) NOT NULL,
        merchant_category       VARCHAR(50),
        amount                  DECIMAL(10, 2),
        transaction_timestamp   TIMESTAMP,
        transaction_country     VARCHAR(3),
        card_present            BOOLEAN,
        is_international        BOOLEAN,
        transaction_hour        INTEGER,
        is_weekend              BOOLEAN,
        fraud_score             DECIMAL(5, 4),
        is_fraud_flagged        BOOLEAN,
        processing_timestamp    TIMESTAMP,
        PRIMARY KEY (transaction_id)
    ) DISTSTYLE KEY DISTKEY (merchant_id) SORTKEY (transaction_timestamp);
  SQL

  depends_on = [aws_redshiftdata_statement.bootstrap_schema]
}

resource "aws_redshiftdata_statement" "bootstrap_dim" {
  count = var.redshift_apply_bootstrap_sql ? 1 : 0

  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  database           = aws_redshift_cluster.this.database_name
  secret_arn         = aws_secretsmanager_secret.redshift_master.arn
  sql                = <<-SQL
    CREATE TABLE IF NOT EXISTS fraud.dim_merchants (
        merchant_id         VARCHAR(50) NOT NULL PRIMARY KEY,
        merchant_name       VARCHAR(256),
        merchant_category   VARCHAR(50),
        risk_level          VARCHAR(20),
        country             VARCHAR(3)
    );
  SQL

  depends_on = [aws_redshiftdata_statement.bootstrap_fact]
}
