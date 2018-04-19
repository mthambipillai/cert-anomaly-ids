# Spark-IDS

Spark-IDS is an anomaly-based and network-based IDS designed on top of Apache Spark for analyzing big amounts of logs stored in Big Data systems like HDFS.
The pipeline is divided into 3 steps: features extraction, anomaly detection and results inspection. Each of these steps is implemented as a separate command and explained in the section 'Usage'.

## Requirements

- You must have `Apache Spark v2.2.1` or higher installed.
- If you want to add new components (see 'Extend' section), you need to have `Scala 2.11` and `sbt` installed.

## Installation

`git clone` the project, `cd` into it and follow one of the following :
- Linux with root privileges : run `sh install-ids.sh`.
- Other cases : extract the files from the `spark-ids.tar.gz` archive, copy `jars/ids.jar` to the root of the project and run it with `spark-submit ids.jar`. Please note that in this case you cannot run it from another directory.