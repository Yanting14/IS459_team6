"""Runs scripts/teardown.sh against stub aws and terraform on PATH.

Nothing here reaches the real account: the stubs only log calls and answer lookups.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("teardown.sh")
PROVIDERS = (SCRIPT.parents[1] / "infra" / "providers.tf").read_text()
ACCOUNT = re.search(r'allowed_account_ids *= *\["(\d+)"\]', PROVIDERS).group(1)
STATE_BUCKET = re.search(r'^ *bucket *= *"([^"]+)"', PROVIDERS, re.M).group(1)
REGION = re.search(r'^ *region *= *"([^"]+)"', PROVIDERS, re.M).group(1)

# On Windows, PATH may find WSL's bash in System32 first; the script targets Git Bash.
BASH = shutil.which("bash")
if BASH and os.name == "nt" and "system32" in BASH.lower():
    GIT = shutil.which("git")
    BASH = str(Path(GIT).parents[1] / "bin" / "bash.exe") if GIT else None
if not BASH or not Path(BASH).exists():
    pytest.skip("No bash found (install Git Bash): teardown.sh tests skipped", allow_module_level=True)

STUB_AWS = """#!/usr/bin/env bash
echo "aws $*" >> "$STUB_LOG"
case "$*" in
  *"sts get-caller-identity"*) echo "$STUB_ACCOUNT" ;;
  *"s3api head-bucket"*) [[ -z "$STUB_NO_BUCKET" ]] || exit 254 ;;
  *"s3api list-object-versions"*) echo '{"Objects": [{"Key": "infra/terraform.tfstate", "VersionId": "v1"}]}' ;;
  *"s3api delete-objects"*) echo "$STUB_DELETE_OUT" ;;
  *"resourcegroupstaggingapi get-resources"*) [[ -z "$STUB_LEFTOVER" ]] || echo "$STUB_LEFTOVER" ;;
esac
"""

STUB_TERRAFORM = """#!/usr/bin/env bash
echo "terraform $*" >> "$STUB_LOG"
[[ "$*" != *"state list"* ]] || [[ -z "$STUB_STATE" ]] || echo "$STUB_STATE"
"""


def run(tmp_path, typed, leftover="", dry_run=True, state="aws_s3_bucket.raw",
        no_bucket=False, delete_out=""):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name, body in {"aws": STUB_AWS, "terraform": STUB_TERRAFORM}.items():
        (bin_dir / name).write_text(body, newline="\n")
        (bin_dir / name).chmod(0o755)
    log = tmp_path / "calls.log"
    log.touch()
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
           "STUB_LOG": str(log), "STUB_ACCOUNT": ACCOUNT, "STUB_LEFTOVER": leftover,
           "STUB_STATE": state, "STUB_NO_BUCKET": "1" if no_bucket else "",
           "STUB_DELETE_OUT": delete_out}
    args = [BASH, str(SCRIPT)] + (["--dry-run"] if dry_run else [])
    result = subprocess.run(args, input=typed + "\n", env=env,
                            capture_output=True, text=True, timeout=60)
    return result, log.read_text()


def test_wrong_account_id_stops_before_any_command(tmp_path):
    result, calls = run(tmp_path, typed="123456789012")

    assert result.returncode != 0
    assert ACCOUNT in result.stdout and REGION in result.stdout
    for step in ("terraform -chdir=infra apply", "terraform -chdir=infra destroy",
                 "delete-objects", "delete-bucket", "get-resources"):
        assert step not in result.stdout + calls


@pytest.mark.parametrize("pad", [" {} ", " {}", "{} "])
def test_typed_id_with_surrounding_spaces_is_refused(tmp_path, pad):
    result, calls = run(tmp_path, typed=pad.format(ACCOUNT))

    assert result.returncode != 0
    assert "terraform -chdir=infra apply" not in result.stdout + calls


def test_steps_run_in_order_and_a_clean_sweep_exits_zero(tmp_path):
    result, _ = run(tmp_path, typed=ACCOUNT)

    assert result.returncode == 0, result.stderr
    out = result.stdout
    steps = ["terraform -chdir=infra apply", "terraform -chdir=infra destroy",
             f"s3api delete-objects --bucket {STATE_BUCKET}",
             f"s3api delete-bucket --bucket {STATE_BUCKET}"]
    positions = [out.find(step) for step in steps]
    assert -1 not in positions, out
    assert positions == sorted(positions), out
    apply_line = out[positions[0]:].splitlines()[0]
    assert "-var force_destroy_buckets=true" in apply_line and "-target=aws_s3_bucket.raw" in apply_line
    assert "-var force_destroy_buckets=true" in out[positions[1]:].splitlines()[0]
    assert out.rfind("Project=is459-team6") > positions[-1]


def test_leftover_tagged_resource_fails_the_sweep(tmp_path):
    arn = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/emr-serverless/x"
    result, _ = run(tmp_path, typed=ACCOUNT, leftover=arn)

    assert result.returncode != 0
    assert arn in result.stdout + result.stderr


def test_state_bucket_is_kept_while_state_still_lists_resources(tmp_path):
    result, calls = run(tmp_path, typed=ACCOUNT, dry_run=False, state="aws_s3_bucket.raw")

    assert result.returncode != 0
    for step in ("list-object-versions", "delete-objects", "delete-bucket"):
        assert step not in calls


def test_rerun_with_empty_state_skips_apply_and_destroy(tmp_path):
    result, calls = run(tmp_path, typed=ACCOUNT, state="")

    assert result.returncode == 0, result.stderr
    for step in ("terraform -chdir=infra apply", "terraform -chdir=infra destroy"):
        assert step not in result.stdout + calls
    assert f"s3api delete-bucket --bucket {STATE_BUCKET}" in result.stdout
    assert "get-resources" in calls


def test_rerun_after_state_bucket_is_gone_goes_straight_to_the_sweep(tmp_path):
    result, calls = run(tmp_path, typed=ACCOUNT, no_bucket=True)

    assert result.returncode == 0, result.stderr
    for step in ("terraform -chdir=infra", "list-object-versions", "delete-bucket"):
        assert step not in result.stdout + calls
    assert "get-resources" in calls


def test_versions_that_fail_to_delete_stop_the_script(tmp_path):
    errors = '{"Errors": [{"Key": "infra/terraform.tfstate", "Code": "AccessDenied"}]}'
    result, calls = run(tmp_path, typed=ACCOUNT, dry_run=False, state="", delete_out=errors)

    assert result.returncode != 0
    assert "AccessDenied" in result.stdout + result.stderr
    assert calls.count("delete-objects") == 1
    assert "delete-bucket" not in calls
