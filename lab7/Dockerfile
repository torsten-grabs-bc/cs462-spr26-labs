# CS462 Lab 7 — Advanced Spark: Media Objects & Spatio-Temporal Data
#
# This image is the Lab 6 Spark image with two extra layers stacked on top:
#   * Apache Sedona (spatial UDTs + ST_* functions) JARs in $SPARK_HOME/jars
#   * Python media libraries (Pillow) + the Sedona Python wrapper
#
# It is fully self-contained: it extends the same public base image Lab 6 used
# and re-applies Lab 6's Hadoop 3.3.6 / S3A swap, so you do NOT need to have
# built the Lab 6 image first.

FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# ---------------------------------------------------------------------------
# Layer 1 — Lab 6 S3A stack: swap Spark's bundled Hadoop 3.3.4 client for
# 3.3.6 (Requester-Pays support) and drop in hadoop-aws + the AWS SDK bundle.
# ---------------------------------------------------------------------------
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367
RUN cd "${SPARK_HOME}/jars" \
 && rm -f hadoop-client-api-*.jar hadoop-client-runtime-*.jar hadoop-aws-*.jar aws-java-sdk-bundle-*.jar \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ---------------------------------------------------------------------------
# Layer 2 — Apache Sedona spatial JARs (shaded build for Spark 3.5 / Scala
# 2.12) plus the geotools-wrapper it depends on. Baking the shaded jar into
# the image means students never wait on spark.jars.packages Ivy resolution
# and never hit a version-mismatch at SparkSession time.
# ---------------------------------------------------------------------------
ARG SEDONA_VERSION=1.8.1
ARG GEOTOOLS_VERSION=1.8.1-33.1
RUN cd "${SPARK_HOME}/jars" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/${SEDONA_VERSION}/sedona-spark-shaded-3.5_2.12-${SEDONA_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${GEOTOOLS_VERSION}/geotools-wrapper-${GEOTOOLS_VERSION}.jar"

# ---------------------------------------------------------------------------
# Layer 3 — Python libraries: Pillow for cracking open images in UDFs, and the
# Sedona Python wrapper. We install apache-sedona WITHOUT the [spark] extra so
# pip does not try to replace the image's pinned PySpark 3.5.3.
# ---------------------------------------------------------------------------
RUN pip install --no-cache-dir \
      "apache-sedona==1.8.1" \
      "shapely>=2.0" \
      "attrs" \
      "Pillow>=10.0"

USER ${NB_UID}
