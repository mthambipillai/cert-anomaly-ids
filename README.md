# Spark-IDS

Spark-IDS is an anomaly-based and network-based IDS designed on top of Apache Spark for analyzing big amounts of logs stored in Big Data systems like HDFS.
The pipeline is divided into 3 steps: features extraction, anomaly detection and results inspection. Each of these steps is implemented as a separate command and explained in the section 'Usage'.

## Requirements

- You must have `Apache Spark v2.2.1` or higher installed.
- If you want to add new components (see 'Extend' section), you need to have `Scala 2.11` and `sbt` installed.

## Installation

`git clone` the project, `cd` into it and follow one of the following :
- Linux with root privileges : run `sh install-ids.sh`.
- Other cases : extract the files from the `spark-ids.tar.gz` archive, copy `jars/ids.jar` to the root of the project and set an alias in your `~/.bashrc` : `alias spark-ids='spark-submit ids.jar'`. Please note that in this case you cannot run it from another directory.

## Usage

You can run 3 different commands :
- `spark-ids extract` : Reads the logs from a specific source and converts them to features according to a specific schema using aggregation for each specific source or destination entity over a specified time window. The features are then scaled and written to a specific parquet file.
- `spark-ids detect` : Reads the previously computed features and apply different anomaly detection algorithms on them to give an anomaly score. The scores are combined into final scores according to a user-defined ensemble technique. The final scores above some specified threshold are considered anomalies. The top n anomalies are written to a csv file.
- `spark-ids inspect` : Reads the previously computed and persisted anomalies and reconstruct the original logs by fetching the original source of logs. A set of user-defined rules are then applied to try to flag the anomalies as true or false positives. The results are then written to a csv file for further investigation.

Each of these commands take parameters that are defined in `conf/application.conf` and can be overriden with command line flags. Execute `spark-ids --help` to see how to use them.