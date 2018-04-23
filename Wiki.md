# Spark-IDS Wiki

## Overview

A description of the IDS's pipeline should give a good overview to understand the role of each of the parameters described in the following sections.

The first step is to `extract` the features from the logs. These logs must be stored in a `.parquet` format. A json file describing the schema is needed for the different steps of the extraction.
In case of testing, we can inject fake intrusions at this stage and persist the fake logs to a separate `.parquet` file for later recall measurement evaluation. Each column of the logs can be of various data types specified by the schema and will be converted to `Double` type.
Aggregation over a time window is then performed to analyze the behaviour of source/destination entities. For example, the total number of connection attempts made by a specific client every hour, or the total number of bytes received by a server per day. To this end, we must choose whether we aggregate the activities of the sources/clients or the destinations/servers. We also need to know how to identify the entities : if there is no authentication of users it's usually done with reverse DNS but if it fails we can fallback to IP addresses or ISPs etc... The schema must also specify the aggregation functions for each column (sum, max, min, etc...).
Once aggregation is done, depending on the size of the time window, the size of the dataset is reduced considerably. We then scale the features for better results, either by rescaling the range of each column between 0.0 and 1.0 or by normalizing each row so that the fields sum up to 1.0. Finally, the features can be persisted in a `.parquet` format.

The second step is to `detect` anomalies from the features. We need to choose a set of detectors among the already implemented ones. Each of them has specific parameters that can be tuned. They will compute anomaly scores between 0.0 and 1.0 for each of the rows. We specify a threshold above which we consider them anomalies. Then we combine the different scores by choosing an ensemble technique. So far, taking the mean or the maximum of the scores are the only two implemented techniques. Finally we sort the anomalies by score and persist the top k to a `.csv` file.

As a last step we need to `inspect` the detected anomalies. Each anomaly has the following fields : the source or destination entity (usually the host resolved by reverse DNS), the timestamp at which the anomaly began, then all the different scaled features values and finally the anomaly score. Based on the size of the time window we can reconstruct the original logs for each anomaly. Then we need to evaluate if the detected anomalies are true or false positives. In the end, the security expert should do it but we can flag the logs with a set of predefined rules that create tags and comments on every matching log. The number of anomalies recognized as true positives by the rules can then be computed to know the precision measurement.
In case the detected anomaly is one of the fake intrusions we injected in the first step, we can mark the intrusion as detected. The number of fake intrusions detected gives us a recall measurement.
Finally, the reconstructed logs with the tags and comments are persisted to one `.csv` file per anomaly.

## Global Parameters

Every parameter has a flag letter and a consistent name between the configuration and the full flag name. So for every entry `param=...` in `conf/application.conf`, there is a corresponding flag `-p, --param <value>`.

The following parameters must be consistent accross the different commands `extract`, `detect` and `inspect` so they must be specified right after `spark-ids`.

- `-f, --featuresschema` : Defines the path and name of the json file of the schema that describes the different columns, their types, aggregation functions, etc... More details about it can be found in the Features Schema subsection.
- `-e, --extractor` : Defines the name of the entity extractor to use (how to define the source and destination entities based on the fields). Every entity extractor needs the original logs schema to contain some specific columns in order to be used. They are specified by the "Requires" list for each of the following already implemented entity extractors :
	- `hostsOnly` : Use only the hostname given by reverse DNS, "NOT_RESOLVED" if it couldn't be resolved. Requires : `["srchost", "dsthost"]`
	- `ipOnly` : Use only IP addresses. Requires : `["srcip", "dstip"]`
	- `hostsWithIpFallback` : Use hostname and fallback to the IP address if it couldn't be resolved. Requires : `["srchost", "dsthost", "srcip", "dstip"]`
	- `hostsWithCountryFallback` : Use hostname and fallback to the country from GeoIP if it couldn't be resolved. Requires : `["srchost","dsthost","srcip_country","dstip_country"]`
	- `hostsWithOrgFallback` : Use hostname and fallback to the ISP if it couldn't be resolved. Requires : `["srchost","dsthost","srcip_org","dstip_org"]`
- `-i, --interval` : Defines the size of the time window for the aggregation and the logs reconstruction. It uses the [Scala duration notation](https://www.scala-lang.org/api/current/scala/concurrent/duration/package$$DurationInt.html) so `60 min`, `1 day` or `8 hours` are all valid.
- `-t, --trafficmode` : In network-based IDSs, aggregated features are called 'traffic features'. This parameter defines whether aggregation is performed per source or destination entity. It can take 2 values : `src` or `dst`.
- `--help` : Prints all the parameters, their usage and a small description in the console.

### Extract

The following parameters can be placed after the `extract` command :

- `-l, --logspath` : Input path for the logs. The logs must be stored in `.parquet` format. The only requirement regarding columns is to have a `timestamp` column. The entity extractors described in the previous subsection might require additional columns. The path accepts wildcards or any other format specific to the file system used. So the example in HDFS the following can all be valid : `todaylogs.parquet`, `januarylogs/*`, `logs/year=2018/*/*/*`, `logs/year=2018/month=0{2,3}/*/*`.
- `-s, --scalemode` : Defines how the features are scaled in order to be used by machine learning algorithms. Currently it accepts two modes :
	- `unit` : Every row is normalized such that the sum of all the fields is 1.0. More info [here](https://en.wikipedia.org/wiki/Feature_scaling#Scaling_to_unit_length).
	- `rescale` : Every feature column is rescaled such that the range is [0.0, 1.0]. More info [here](https://en.wikipedia.org/wiki/Feature_scaling#Rescaling).
- `-f, --featuresfile` : Defines the path and name of the file where to write the features to. The extension is `.parquet` and is added automatically to the path. So `featuresfile=myfeatures` will create `myfeatures.parquet`.
- `-r, --recall` : Must be set to `true` if we are testing some model and want to inject intrusions to compute recall in the `inspect` step, `false` otherwise.
- `-i, --intrusions` : Defines the path and name of the json file that describes the intrusions, the number of instances for each intrusion kind. More details about it can be found in the Intrusions subsection. This parameter has no effect if `recall=false`.
- `-d, --intrusionsdir` : Defines the path of the directory where to persist the computed intrusions and their corresponding fake logs. This parameter has no effect if `recall=false`.

### Detect

The following parameters can be placed after the `detect` command :

- `-f, --featuresfile` : Defines the path and name of the file where to read the features from. The extension is `.parquet` and is added automatically to the path. So `featuresfile=myfeatures` will try to read from `myfeatures.parquet`. The same parameter is defined for features creation in `extract` but in `conf/application.conf` it is a single parameter.
- `-d, --detectors` : Defines the detectors to be used for anomaly detection in the following format : `"d1,d2,d3,..."` where `d1`, `d2` and `d3` are the names of the detectors. So far two detectors have been implemented : `iforest` (described in IsolationForest subsection) and `kmeans` (described in KMeans subsection). A third detector using Local Outlier Factor and named `lof` should be implemented soon.
- `-t, --threshold` : Defines the threshold score value between 0.0 and 1.0 above which aggregated rows are considered anomalies.
- `-n, --nbtopanomalies` : Defines how many anomalies should be persisted, starting from the anomaly with the highest score and going down in the sorted scores of the detected anomalies.
- `-a, --anomaliesfile` : Defines the path and name of the file where to write the anomalies to. The extension is `.csv` and is added automatically to the path. So `anomaliesfile=myanomalies` will create `myanomalies.csv`.
- `-e, --ensemblemode` : Defines the ensemble technique used to combine the scores of the different detectors. Currently two techniques are implemented : `mean` and `max`. Their names are self-explanatory.

### Inspect

The following parameters can be placed after the `inspect` command :

- `-a, --anomaliesfile` : Defines the path and name of the file where to read the anomalies from. The extension is `.csv` and is added automatically to the path. So `anomaliesfile=myanomalies` will try to read `myanomalies.csv`. The same parameter is defined for anomalies detection in `detect` but in `conf/application.conf` it is a single parameter.
- `-r, --rules` : Defines the path and name of the json file that describes the inspection rules and their parameters. More details about it can be found in the Rules subsection.
- `-i, --inspectionfiles` : Defines the path and name of the files where to write the anomalies to. If the amount of logs per anomaly is not too large, there will be one file per anomaly, otherwise some anomaly logs might be split in different files. The extension is `.csv` and is added automatically to the path. So `inspectionfiles=myinspection` will create files like `myinspection-part0001.csv`.
- `-d, --intrusionsdir` : Defines the path of the folder where to read the injected intrusions and their logs from in case we want to compute recall. This parameter has no effect if `recall=false`. The same parameter is defined for intrusions injection in `extract` but in `conf/application.conf` it is a single parameter.

### Features Schema

The features schema is provided as a json file. The path and name of this file are described by the `featuresschema` parameter. A sample schema can be found in `conf/BroSSHfeatures.json`.

A schema has 2 fields : `name`, which simply defines the name of the schema, and `features` which is an array of features where each feature element has the following structure :
```
{
	"name" : "featurename",
	"parent" : "parentfeature"|null,
	"type" : "Datatype",
	"doc" : "Description of the feature",
	"aggs" : ["agg1","agg2","agg3",...]
}
```

- `name` : Features can be either 'top' features that are columns in the original logs schema (in which case this field must match one of the columns' names), or they can be computed from another feature in the schema.
- `parent` : In the case the feature is computed from another feature, this field defines the name of that parent feature. In the case the feature is a 'top' feature this field is `null`.
- `type` : Describes the data type of the feature so that we know how to parse it to `Double`. Currently supported values are : `["Boolean", "Int", "String", "Long", "Host", "Day", "Hour"]`. The `"Host"` type should be used when the column defines a DNS hostname, `"Day"` and `"Hour"` should be used when the column contains UNIX timestamps in milliseconds as `Long`s.
- `doc` : Relevant documentation for the feature
- `aggs` : List of aggregation functions to use to extract traffic features in the `extract` phase. Possible values are : `["mostcommon", "countdistinct", "mean", "sum", "max", "min"]`.

### Intrusions

### Rules

## Detector-specific Parameters

### IsolationForest

### KMeans