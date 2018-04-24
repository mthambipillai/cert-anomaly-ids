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
- `doc` : Relevant documentation for the feature.
- `aggs` : List of aggregation functions to use to extract traffic features in the `extract` phase. Possible values are : `["mostcommon", "countdistinct", "mean", "sum", "max", "min"]`.

### Intrusions

The intrusions to inject are defined in a json file. The path and name of this file are described by the `intrusions` parameter. A sample description of intrusions can be found in `conf/intrusions.json`.

An intrusion kind defines how intrusions are injected, on which fields, etc. An intrusion is the set of resulting fake logs after injecting an intrusion kind. The intrusion comes with a signature and a begin and end timestamps. The time range of the original logs will define the range of injections for each intrusion kind, but the implementation of the intrusion kind itself will decide where exactly to inject the intrusions.

Like the features schema, it has 2 fields : `name`, which simply defines the name of the intrusions schema, and `intrusions` which is an array of intrusions kinds where each intrusion kind element has the following structure :

```
{
	"name" : "intrusionkindname",
	"doc" : "Description of the intrusion kind",
	"requiredcolumns" : ["col1", "col2", ...],
	"number" : 3
}
```

- `name` : The name of the intrusion kind which must be already implemented with that name. For now there is only one implemented intrusion kind : `tooManyAuthAttempts`, which creates some connections of a same fake source host, each one with a very high number of attempts.
- `doc` : Relevant documentation for the intrusion kind.
- `requiredcolumns` : List of columns of the original logs schema that must be present because the intrusion kind will use these fields.
- `number` : Defines how many intrusions of that kind should be injected in the logs.

### Rules

The rules for the inspection are defined in a json file. The path and name of this file are described by the `rules` parameter. A sample schema can be found in `conf/rules.json`.

Each rule can be either a 'simple rule' or a 'complex rule'. A simple rule will analyze each log entry of the anomaly independantly and check if it matches the rule. A complex rule will analyze all log entries of the anomaly with an aggregation function and check if the result of the aggregation matches the rule.

Two columns will be added to the original logs schema : `anomalytag` and `comments`. The `anomalytag` column is always empty for every log entry except the first one of the anomaly and it can take 2 values : "yes" or "?". "yes" means that at least one of the log entries matched at least one of the rule so we know the anomaly is a true positive according to the rules. "?" means that no log matched any of the rules so we don't know if the anomaly is a true or false positive, the user should investigate himself/herself.
The `comments` column is defined for every log entry that matched at least one of the rules. Each rule that matches adds its own text to the field, describing what makes this log abnormal.

A rules' file has 2 fields : `name`, which simply defines the name of the rules' schema, and `rules` which is an array of rules where each rule element has the following structure :

```
{
    "name" : "rulename",
    "params" : ["param1", "param2", ...],
    "text" : "comment text"
  }
```

- `name` : The name of the rule which must be already implemented with that name. See below a list of already implemented rules.
- `params` : Defines a list of rule-specific string parameters for the rule. They might be filenames or integers that must be parsed in the application or a specific text to pattern match on, etc...
- "text" : In case the rule finds a match, defines the text to add in the `comments` column to describe why the log was flagged. 

The following rules are already implemented :
- `"ssh_auth_attempts"` : Checks if the number of attempts in a SSH connection is higher than some value defined by the first parameter in `params`.
- `"ssh_dstport"` : Checks if the destination port in a SSH connection is not 22. No parameters are needed.
- `"ssh_version"` : Checks if the version number in a SSH connection is less than the version number described by the first parameter.
- `"ssh_srcip"` : Checks if the source IP address belongs to a list of known malicious IP addresses. The first parameter should contain the file path and name where these addresses are stored, one per line.
- `"ssh_dsthost"` : Checks if the destination hostname is very uncommon by looking in a file of most common destination hosts. The first parameter should contain the file path and name where these hosts are stored, one per line in the following format `hostname|||count` where `count` is the number of times this hostname was seen in some log dataset used to compute these statistics.
- `"ssh_client"` : Checks if the client software used for the SSH connection is very uncommon by looking in a file of most common client softwares. The first parameter should contain the file path and name where these clients are stored, one per line in the following format `client|||count` where `count` is the number of times this client was seen in some log dataset used to compute these statistics.
- `"ssh_server"` : Similar to `"ssh_client"` but this time for server software.
- `"ssh_cipher"` : Checks if the cipher algorithm used for the SSH connection is very uncommon by looking in a file of most common ciphers. The first parameter should contain the file path and name where these ciphers are stored, one per line in the following format `cipher|||count` where `count` is the number of times this cipher was seen in some log dataset used to compute these statistics.
- `"ssh_total_auth_attempts"` : Checks if the number of attempts accross all SSH connections in the anomaly is higher than some value defined by the first parameter in `params`.

Note that the statistics files for `"ssh_srcip"`, `"ssh_dsthost"`, `"ssh_client"`, `"ssh_server"` and `"ssh_cipher"` are not provided in this repository as they contain sensitive and environment-dependant data.

## Detector-specific Parameters

Each of the already implemented detectors has parameters to tune the efficiency of the machine learning algorithm. These parameters cannot be changed through the command line interface but only in the `conf/application.conf` file.

### IsolationForest

IsolationForest is an anomaly detection algorithm adapted from the RandomForest algorithm. The main idea is that if a point is isolated in a data space, its path in a decision tree will be much shorter because after a few splits it becomes the only data point in that subspace and no further splitting is possible. IsolationForest creates a number of IsolationTrees, each one takes a set of samples from the data to compute the splitting decisions. Then each point in the dataset goes through each tree to compute the average path length which are then mapped to anomaly scores such that shorter path lengths have scores closer to 1.0.
More details about IsolationForest can be found [here](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf).

The following parameters can be tuned :

- `nbtrees` : The number of IsolationTrees to use.
- `nbsamples` : The number of samples to use to build an IsolationTree.

### KMeans

KMeans is a clustering algorithm that divides the dataset into a specific number of clusters. The clusters are computed from a subset of the data (trainset) and the rest of the data can be assigned to the already computed clusters. Points inside clusters with sizes smaller than a specific value can be considered anomalies with 1.0 scores. Similarly, points inside clusters with sizes greater than a specific value can be considered normal with 0.0 scores. All points in cluster with sizes between these 2 boundaries can be linearly mapped to scores between 0.0 and 1.0 according to the cluster size.
The number of cluster can be set explicitly or computed with the elbow technique : we apply the algorithm to the train set iteratively from a minimum number of clusters to a maximum number of clusters, each time computing the within-cluster sum of squared errors (WSSE), according to the elbow technique the WSSE should decrease at a fast rate until at some point it 'suddenly' decreases a lot slower. The number of clusers at this point should be the optimal number of clusters.
More details about KMeans can be found [here](https://en.wikipedia.org/wiki/K-means_clustering).

The following parameters can be tuned :

- `trainratio` : The percentage value between 0.0 and 1.0 of the data to be used as a trainset.
- `minnbk` : The minimum number of clusters when using the elbow technique.
- `maxnbk` : The maximum number of clusters when using the elbow technique.
- `elbowratio` : Defines the ratio between 0.0 and 1.0 at which the WSSE should decrease 'fast', when the ratio hits a value below this threshold, the current number of clusters is used.
- `nbk` : The number of clusters to use in case we want to set it explicitly, should be -1 if we want to use the elbow technique instead.
- `lowbound` : Defines the cluster size below which points are considered anomalies with 1.0 score.
- `upbound` : Defines the cluster size above which points are considered normal with 0.0 score.

## Contributing

You can contribute by adding new features functions, detectors, rules, etc to the ones already implemented.

Here are a few possible extensions :
- Create a new aggregate function for features : in `features/Feature.scala`, in the companion `object`, implement a new function of type `String => Column` where the `String` is the column name. You can use already predefined aggregation functions from `import org.apache.spark.sql.functions._` or you can build your own by extending the class `UserDefinedAggregateFunction`. Once you have this function, you can add a `case` in the `getAggFunction` method in `features/FeaturesParser.scala` using a name of your choice. This name will be string you have to specify in the `"aggs"` array in the json file of the features schema in order to use your function.
- Create a new entity extractor : in `features/EntityExtractor.scala`, in the `object` create a new `EntityExtractor` as a `val` and add it to the list `extractors`. The `name` field of your extractor will be the string you have to specify in `conf/application.conf` or with the `--extractor` flag in order to use it.
- Create a new kind of intrusion/anomaly : in `evaluation/IntrusionKind.scala`, in the `object` create a new function as a `val` of type `String => IntrusionKind` where the input string is the description of the intrusion kind. Then add it to the list `allKinds`. The `name` field of your intrusion kind will be the string you have to use as name in the json file specified by `intrusions` in `conf/application.conf` or by the `--intrusions` flag.
- Create a new anomaly detection algorithm : the main class of your implementation must extend the `detection.Detector` class. Then in the `object` in `detection/Detector.scala`, add a `case` to return a new instance of your `Detector` in the method `getDetector`. The string of the pattern match should be the name of your `Detector` that you will specify in the `detectors` field in `conf/application.conf` or in the `--detectors` flag.
- Create a new inspection rule : in `inspection/RulesParser.scala`, add a `case` to return your `Rule` in the method `getRule`. The string of the pattern match should be the name of your `Rule` that you have to use in the json file specified by `rules` in `conf/application.conf` or by the `--rules` flag. The parameters of the rule are defined in `params: List[String]`, if you need to parse them to some other data type like `Int`, use proper error handling as in the already implemented rules that involve parsing. `Rule` is abstract so you must implement either a `SimpleRule` (over single log entry) or a `ComplexRule` (over all logs of the anomaly). In case of `SimpleRule`, you should use the generic method `makeSimpleRule` defined in the companion `object`.