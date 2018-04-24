# Spark-IDS

Spark-IDS is an anomaly-based and network-based IDS designed on top of Apache Spark for analyzing big amounts of logs stored in Big Data systems like HDFS.
The pipeline is divided into 3 steps: features extraction, anomaly detection and results inspection. Each of these steps is implemented as a separate command and explained in the section 'Usage'.

## Requirements

- You must have `Apache Spark v2.2.1` or higher installed.
- If you want to add new components (see 'Contributing' section), you need to have `Scala 2.11` and `sbt` installed.

## Installation

`git clone` the project, `cd` into it and follow one of the following :
- Linux with root privileges : run `sh install-ids.sh`.
- Other cases : extract the files from the `spark-ids.tar.gz` archive, copy `jars/ids.jar` to the root of the project and set an alias in your `~/.bashrc` : `alias spark-ids='spark-submit ids.jar'`. Please note that in this case you cannot run it from another directory.

## Usage

You can run 3 different commands :
- `spark-ids extract` : Reads the logs from a specific source and converts them to features according to a specific schema using aggregation for each specific source or destination entity over a specified time window. The features are then scaled and written to a specific parquet file.
- `spark-ids detect` : Reads the previously computed features and apply different anomaly detection algorithms on them to give an anomaly score. The scores are combined into final scores according to a user-defined ensemble technique. The final scores above some specified threshold are considered anomalies. The top n anomalies are written to a csv file.
- `spark-ids inspect` : Reads the previously computed and persisted anomalies and reconstruct the original logs by fetching the original source of logs. A set of user-defined rules are then applied to try to flag the anomalies as true or false positives. The results are then written to csv files for further investigation.

Each of these commands take parameters that are defined in `conf/application.conf` and can be overriden with command line flags. Execute `spark-ids --help` to see how to use them.
You can find more information about every parameter in the [wiki](https://gitlab.cern.ch/mthambip/IDSv2.2/blob/master/Wiki.md).

## Example

Consider the following directory structure :
```
.
├── _logs
|   ├── broSSHlogs-jan.parquet
|   ├── broSSHlogs-feb.parquet
├── _features
├── _anomalies
├── _inspections
```
We first extract features from the logs :

`spark-ids extract -l logs/* -f features/featuresSSH`
```
.
├── _logs
|   ├── broSSHlogs-jan.parquet
|   ├── broSSHlogs-feb.parquet
├── _features
|   ├── featuresSSH.parquet
├── _anomalies
├── _inspections
```
Then we use only IsolationForest and KMeans as detectors with a new threshold and only the top 3 anomalies :

`spark-ids detect -f features/features -d "iforest,kmeans" -t 0.75 -n 3 -a anomalies/anomaliesSSH`
```
.
├── _logs
|   ├── broSSHlogs-jan.parquet
|   ├── broSSHlogs-feb.parquet
├── _features
|   ├── featuresSSH.parquet
├── _anomalies
|   ├── anomaliesSSH.csv
├── _inspections
```
Finally we inspect the detected anomalies and get one csv file per anomaly :

`spark-ids inspect -a anomalies/anomalies.csv -i inspections/inspectionresultsSSH`
```
.
├── _logs
|   ├── broSSHlogs-jan.parquet
|   ├── broSSHlogs-feb.parquet
├── _features
|   ├── featuresSSH.parquet
├── _anomalies
|   ├── anomaliesSSH.csv
├── _inspections
|   ├── inspectionresultsSSH-part0000.csv
|   ├── inspectionresultsSSH-part0001.csv
|   ├── inspectionresultsSSH-part0002.csv
```

All the other parameters that we didn't explicitly set as flags were defined in `$SPARK_IDS_HOME/conf`.

## Contributing

For any change to the code, you need to rebuild the project with `sbt assembly`. If you installed `spark-ids` on your machine, you then need to copy the newly computed jar to the installed jar :

`sudo cp target/scala-2.11/IDS\ Project-assembly-2.0.jar $SPARK_IDS_HOME/jars/ids.jar`

Check out the Contributing section in the [wiki](https://gitlab.cern.ch/mthambip/IDSv2.2/blob/master/Wiki.md) for more details about possible extensions.