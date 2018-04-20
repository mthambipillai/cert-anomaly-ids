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

### Extract

### Detect

### Inspect

### Features Schema

### Intrusions

### Rules

## Detector-specific Parameters

### IsolationForest

### KMeans