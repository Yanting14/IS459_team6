"""Runs scripts/teardown.sh --dry-run against stub aws and terraform on PATH.

Nothing here reaches the real account: the stubs only answer the read-only lookups.
"""

import os
import shutil
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).with_name("teardown.sh")
ACCOUNT = "207567757101"

# On Windows, PATH may find WSL's bash in System32 first; the script targets Git Bash.
BASH = shutil.which("bash")
if os.name == "nt" and "system32" in BASH.lower():
    BASH = str(Path(shutil.which("git")).parents[1] / "bin" / "bash.exe")

STUB_AWS = """#!/usr/bin/env bash
echo "aws $*" >> "$STUB_LOG"
case "$*" in
  *"sts get-caller-identity"*) echo "$STUB_ACCOUNT" ;;
  *"s3api list-object-versions"*) echo '{"Objects": [{"Key": "infra/terraform.tfstate", "VersionId": "v1"}]}' ;;
  *"resourcegroupstaggingapi get-resources"*) [[ -z "$STUB_LEFTOVER" ]] || echo "$STUB_LEFTOVER" ;;
esac
"""

STUB_TERRAFORM = """#!/usr/bin/env bash
echo "terraform $*" >> "$STUB_LOG"
[[ "$*" != *"output -raw region"* ]] || echo us-east-1
"""


def run(tmp_path, typed, leftover=""):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name, body in {"aws": STUB_AWS, "terraform": STUB_TERRAFORM}.items():
        (bin_dir / name).write_text(body, newline="\n")
        (bin_dir / name).chmod(0o755)
    log = tmp_path / "calls.log"
    log.touch()
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
           "STUB_LOG": str(log), "STUB_ACCOUNT": ACCOUNT, "STUB_LEFTOVER": leftover}
    result = subprocess.run([BASH, str(SCRIPT), "--dry-run"], input=typed + "\n", env=env,
                            capture_output=True, text=True, timeout=60)
    return result, log.read_text()


def test_wrong_account_id_stops_before_any_command(tmp_path):
    result, calls = run(tmp_path, typed="123456789012")

    assert result.returncode != 0
    assert ACCOUNT in result.stdout and "us-east-1" in result.stdout
    for step in ("terraform -chdir=infra apply", "terraform -chdir=infra destroy",
                 "delete-objects", "delete-bucket", "get-resources"):
        assert step not in result.stdout + calls


def test_steps_run_in_order_and_a_clean_sweep_exits_zero(tmp_path):
    result, _ = run(tmp_path, typed=ACCOUNT)

    assert result.returncode == 0, result.stderr
    out = result.stdout
    steps = ["terraform -chdir=infra apply", "terraform -chdir=infra destroy",
             "s3api delete-objects --bucket is459-team6-tfstate-207567757101",
             "s3api delete-bucket --bucket is459-team6-tfstate-207567757101"]
    positions = [out.find(step) for step in steps]
    assert -1 not in positions, out
    assert positions == sorted(positions), out
    apply_line = out[positions[0]:].splitlines()[0]
    assert "-var force_destroy_buckets=true" in apply_line and "-target=aws_s3_bucket.raw" in apply_line
    assert "-var force_destroy_buckets=true" in out[positions[1]:].splitlines()[0]
    assert out.rfind("Project=is459-team6") > positions[-1]


def test_leftover_tagged_resource_fails_the_sweep(tmp_path):
    arn = "arn:aws:logs:us-east-1:207567757101:log-group:/aws/emr-serverless/x"
    result, _ = run(tmp_path, typed=ACCOUNT, leftover=arn)

    assert result.returncode != 0
    assert arn in result.stdout + result.stderr
