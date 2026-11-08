import os
import subprocess
import sys
from pathlib import Path

import pyspark
import pytest
from pyspark.sql import SparkSession

PREPARE = Path(__file__).parent.parent / "prepare"
# The constants test compares against prepare.py itself.
sys.path.insert(0, str(PREPARE))


def windows_filesystem(tmp: Path) -> dict:
    """Hadoop's local filesystem needs native binaries on Windows; compile a shim instead."""
    jars = Path(pyspark.__file__).parent / "jars" / "*"
    src = PREPARE / "WindowsLocalFileSystem.java"
    subprocess.run(["javac", "-nowarn", "-cp", str(jars), "-d", str(tmp), str(src)], check=True)
    return {"spark.driver.extraClassPath": str(tmp),
            "spark.hadoop.fs.file.impl": "WindowsLocalFileSystem"}


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (SparkSession.builder.master("local[1]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    if os.name == "nt":
        for key, value in windows_filesystem(tmp_path_factory.mktemp("hadoop")).items():
            builder = builder.config(key, value)
    session = builder.getOrCreate()
    yield session
    session.stop()
