"""Before any BQ1 training on EMR: Spark ML, numpy and Python on the driver and an executor."""
import sys, numpy, pyspark
from pyspark.ml.classification import GBTClassifier, LogisticRegression  # noqa: F401
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("bq1-smoke").getOrCreate()
print("driver", sys.version.split()[0], "numpy", numpy.__version__, "pyspark", pyspark.__version__, "spark", spark.version)
print("executor", spark.sparkContext.parallelize([0], 1).map(lambda _: (sys.version.split()[0], __import__("numpy").__version__)).collect())
