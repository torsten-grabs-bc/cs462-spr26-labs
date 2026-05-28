# Lab 6 — custom Spark image for CS 462
#
# Why this Dockerfile exists:
#   Spark 3.5.3 ships hadoop-client-api/runtime 3.3.4 in $SPARK_HOME/jars. Reading
#   Requester-Pays buckets via the s3a:// connector requires hadoop-aws >= 3.3.5,
#   which depends on classes (PrefetchingStatistics et al.) that live in
#   hadoop-common 3.3.5+. Mixing the bundled 3.3.4 client with hadoop-aws 3.3.6
#   at runtime via `spark.jars.packages` throws NoClassDefFoundError because the
#   bundled JARs take classpath precedence.
#
#   Fix: replace the bundled Hadoop client JARs with 3.3.6 versions at image
#   build time, and drop in hadoop-aws + aws-java-sdk-bundle alongside. Then
#   SparkSession needs no `spark.jars.packages` config at runtime and starts in
#   5–10 seconds instead of 30+ s.

FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367

RUN cd "${SPARK_HOME}/jars" \
 && rm -f hadoop-client-api-*.jar hadoop-client-runtime-*.jar hadoop-aws-*.jar aws-java-sdk-bundle-*.jar \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
 && curl -fsSL -O "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

USER ${NB_UID}
