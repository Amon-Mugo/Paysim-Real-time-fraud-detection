# used for loacl testing and runs using hadoop to download the iceberg runtime JAR package and tests them
# and store tables locally in designated warehouse
# Production EMR Serveless Glue catalog this leverage AWS EMR per bundled iceberg runtime package runtime
# (when the intenet access restricted meaning no package can be downloaded)
# points the warehouse to an s3 bucket using aws glue data catalog

from __future__ import annotations
import pyspark
from spark.config import DetectionConfig

ICEBERG_VERSION = "1.10.1"
SUPPORTED_SPARK_MINORS = ("4.0", "4.1")  # limits compiled spark minor versions
SCALA_VERSION = "2.13"  # scala version for runtime
_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
# used to register the iceberg custom sql extension such as merg,call
_SPARK_CATALOG_IMPL = (
    "org.apache.iceberg.spark.SparkCatalog"  # spark required interface to iceberg
)
_GLUE_CATALOG_IMPL = (
    "org.apache.iceberg.aws.glue.GlueCatalog"  # spark required interface for aws glue
)
_S3_FILE_IO_IMPL = (
    "org.apache.iceberg.aws.s3.S3FileIO"  # spark interface for amazon s3 storage
)


# used to detect major and minor versions of pyspark library currents istalled in environment
def _spark_minor_version() -> str:
    return ".".join(pyspark.__version__.split(".")[:2])


# used to verify the versions of spark and scala
def _runtime_packages() -> str:
    spark_minor = _spark_minor_version()
    if spark_minor not in SUPPORTED_SPARK_MINORS:
        raise ValueError(
            f"No Iceberg {ICEBERG_VERSION} runtime for Spark {spark_minor}; "
            f"supported Spark lines: {', '.join(SUPPORTED_SPARK_MINORS)}"
        )
    return (
        f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor}_{SCALA_VERSION}"
        f":{ICEBERG_VERSION}"
    )


# used to generate the configaration path for apache iceberg when using local Hadoop based catalog
# used for testing locally only no deployment
def _hadoop_catalog_properties(config: DetectionConfig) -> dict[str, str]:
    return {
        "type": "hadoop",
        "warehouse": config.iceberg_warehouse,
    }  # this will gives us the temporary file path where the iceberg will leave


# used by apache icberg configuration when using aws glue data catalog used for production environment like aws emr serveless
def _glue_catalog_properties(config: DetectionConfig) -> dict[str, str]:
    if not config.iceberg_warehouse.startswith("s3://"):
        raise ValueError("The glue catalog needs s3:// ICEBERG-WAREHOUSE")
    return {
        "catalog-impl": _GLUE_CATALOG_IMPL,  # tell iceberg to use aws glue as database and table insted as locla directory
        "io-impl": _S3_FILE_IO_IMPL,  # used for reading and writing data in s3 and also connection
        "warehouse": config.iceberg_warehouse,  # set project root where it will leave
    }


# used to download maven package if it exist or not as an external package
def iceberg_packages(config: DetectionConfig) -> list[str]:
    if config.iceberg_catalog_type == "glue":
        return []  # means don't download anythig
    return [_runtime_packages()]


# used to generate a complete directory of spark config setting required for successful register and
# initilise apache iceberg in pyspark session
def iceberg_spark_conf(config: DetectionConfig) -> dict[str, str]:
    if config.iceberg_catalog_type == "glue":
        properties = _glue_catalog_properties(config)
    else:
        properties = _hadoop_catalog_properties(config)

    catalog_prefix = f"spark.sql.catalog.{config.iceberg_catalog_name}"

    conf = {
        "spark.sql.extensions": _EXTENSIONS,  # register advance sql configs eg merg into
        catalog_prefix: _SPARK_CATALOG_IMPL,
    }
    conf.update({f"{catalog_prefix}.{key}": value for key, value in properties.items()})
    return conf
