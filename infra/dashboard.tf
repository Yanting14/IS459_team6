# The Streamlit dashboard on one EC2 instance, off unless enable_dashboard. Plain HTTP on 8501
# from anywhere, deliberately: the data is synthetic and the shared password gates the app.

locals {
  dashboard_count = var.enable_dashboard ? 1 : 0
}

# Terraform creates the secret but never its value, so the password stays out of state.
# Fill it once with: aws secretsmanager put-secret-value (see docs/run-pipeline.md).
resource "aws_secretsmanager_secret" "dashboard_password" {
  name                    = "${var.project}/dashboard-password"
  description             = "Shared password for the hosted dashboard, as a plain string"
  recovery_window_in_days = 0
}

# --- Code -----------------------------------------------------------------------

data "archive_file" "dashboard" {
  count            = local.dashboard_count
  type             = "zip"
  source_dir       = "${path.module}/../dashboard"
  output_path      = "${path.module}/build/dashboard.zip"
  output_file_mode = "0666"
  excludes = [
    "tests", "tests/**", ".streamlit/secrets.toml", "data", "data/**", ".pytest_cache", ".pytest_cache/**",
    "__pycache__", "**/__pycache__", "**/__pycache__/**",
  ]
}

resource "aws_s3_object" "dashboard_code" {
  count  = local.dashboard_count
  bucket = aws_s3_bucket.scoring["gold"].id
  key    = "code/dashboard.zip"
  source = data.archive_file.dashboard[0].output_path
  etag   = data.archive_file.dashboard[0].output_md5
}

# --- Network --------------------------------------------------------------------

resource "aws_security_group" "dashboard" {
  count       = local.dashboard_count
  name        = "${var.project}-dashboard"
  description = "Dashboard: inbound 8501, outbound HTTPS only, no SSH"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Streamlit"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "HTTPS to S3, Athena, Secrets Manager, SSM, package mirrors"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- IAM ------------------------------------------------------------------------

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dashboard" {
  count              = local.dashboard_count
  name               = "${var.project}-dashboard"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "dashboard" {
  for_each = var.enable_dashboard ? {
    athena = aws_iam_policy.athena_query.arn
    ssm    = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  } : {}
  role       = aws_iam_role.dashboard[0].name
  policy_arn = each.value
}

resource "aws_iam_role_policy" "dashboard" {
  count = local.dashboard_count
  name  = "read-code-and-password"
  role  = aws_iam_role.dashboard[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.scoring["gold"].arn}/${aws_s3_object.dashboard_code[0].key}"
      },
      {
        # The Review queue page reads the BQ1 model's metrics.json straight from S3; athena_query covers only curated and gold.
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${local.models_bq1}/*/metrics.json"
      },
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.dashboard_password.arn
      },
    ]
  })
}

resource "aws_iam_instance_profile" "dashboard" {
  count = local.dashboard_count
  name  = "${var.project}-dashboard"
  role  = aws_iam_role.dashboard[0].name
}

# --- Instance -------------------------------------------------------------------

# Not every AZ offers every instance type (us-east-1e has no t3), so use a default subnet in one that does.
data "aws_ec2_instance_type_offerings" "dashboard" {
  count         = local.dashboard_count
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.dashboard_instance_type]
  }
}

data "aws_subnets" "dashboard" {
  count = local.dashboard_count
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
  filter {
    name   = "availability-zone"
    values = data.aws_ec2_instance_type_offerings.dashboard[0].locations
  }
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_instance" "dashboard" {
  count                       = local.dashboard_count
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.dashboard_instance_type
  subnet_id                   = sort(data.aws_subnets.dashboard[0].ids)[0]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.dashboard[0].id]
  iam_instance_profile        = aws_iam_instance_profile.dashboard[0].name

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  credit_specification {
    cpu_credits = "standard"
  }

  root_block_device {
    volume_type = "gp3"
    encrypted   = true
  }

  # A new AL2023 release must not replace a running demo host; code changes still do (user_data below).
  lifecycle {
    ignore_changes = [ami]
  }

  # The zip's hash is in the script, so a code change replaces the instance on apply.
  user_data_replace_on_change = true
  user_data                   = <<-EOT
    #!/bin/bash
    # code ${data.archive_file.dashboard[0].output_base64sha256}
    set -euo pipefail
    dnf install -y python3.11 unzip
    # Unprivileged service user; Streamlit writes ~/.streamlit, so it needs a home.
    useradd --system --create-home --shell /sbin/nologin dashboard
    mkdir -p /opt/dashboard
    aws s3 cp --region ${var.region} s3://${aws_s3_object.dashboard_code[0].bucket}/${aws_s3_object.dashboard_code[0].key} /tmp/dashboard.zip
    unzip -o /tmp/dashboard.zip -d /opt/dashboard
    python3.11 -m venv /opt/dashboard/venv
    /opt/dashboard/venv/bin/pip install -r /opt/dashboard/requirements.txt

    cat > /opt/dashboard/run.sh <<'RUN'
    #!/bin/bash
    set -euo pipefail
    DASH_PASSWORD=$(aws secretsmanager get-secret-value --region ${var.region} --secret-id ${aws_secretsmanager_secret.dashboard_password.arn} --query SecretString --output text || true)
    # Strip exactly as app.py does, so a whitespace-only secret fails closed instead of disabling the gate.
    DASH_PASSWORD=$(printf '%s' "$DASH_PASSWORD" | /opt/dashboard/venv/bin/python -c 'import sys; print(sys.stdin.read().strip(), end="")')
    # Fail closed: never serve the dashboard without its password.
    if [ -z "$DASH_PASSWORD" ] || [ "$DASH_PASSWORD" = "None" ]; then echo "dashboard password secret is empty" >&2; exit 1; fi
    export DASH_PASSWORD DASH_SOURCE=athena DASH_ATHENA_REGION=${var.region} DASH_ATHENA_WORKGROUP=${local.athena_workgroup} DASH_ATHENA_DATABASE=${local.athena_database}
    cd /opt/dashboard
    exec venv/bin/streamlit run app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
    RUN
    chmod 755 /opt/dashboard/run.sh

    cat > /etc/systemd/system/dashboard.service <<'UNIT'
    [Unit]
    Description=AML review dashboard
    After=network-online.target
    Wants=network-online.target

    [Service]
    User=dashboard
    ExecStart=/opt/dashboard/run.sh
    Restart=on-failure
    RestartSec=30

    [Install]
    WantedBy=multi-user.target
    UNIT
    systemctl daemon-reload
    systemctl enable --now dashboard.service
  EOT
}
