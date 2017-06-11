# Benchmark results

# Raw data

## RDD

### Sample size = 100 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.count()|0.0|0.0|0.0|0.0|4.656|
|RDD.distinct()|0.013|0.02|0.073|0.028|19.613|
|RDD.filter()|0.004|0.008|0.139|0.008|7.374|
|RDD.flatMap()|0.042|0.045|0.178|0.022|7.059|
|RDD.map()|0.005|0.004|0.115|0.006|7.098|
|RDD.map().reduce()|0.006|0.005|0.066|0.005|5.756|
|RDD.sortBy()|0.075|0.058|0.084|0.039|18.906|
|RDD.take()|0.001|0.002|0.038|0.002|7.9|
|RDD.union()|0.01|0.018|0.151|0.032|8.459|
|RDD.zip()|0.006|0.013|0.195|0.008|7.573|
|RDD.zipWithIndex()|0.013|0.015|0.071|0.01|6.004|

### Sample size = 1000 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.count()|0.0|0.0|0.0|0.0|6.418|
|RDD.distinct()|0.187|0.27|0.242|0.281|35.137|
|RDD.filter()|0.037|0.035|0.204|0.036|10.158|
|RDD.flatMap()|0.412|0.538|0.377|0.208|11.303|
|RDD.map()|0.05|0.036|0.16|0.049|8.381|
|RDD.map().reduce()|0.057|0.043|0.241|0.042|7.273|
|RDD.sortBy()|0.681|0.754|0.376|0.468|31.205|
|RDD.take()|0.001|0.001|0.049|0.001|8.495|
|RDD.union()|0.171|0.289|0.398|0.256|13.555|
|RDD.zip()|0.053|0.088|0.223|0.055|11.453|
|RDD.zipWithIndex()|0.079|0.109|0.167|0.083|10.813|

### Sample size = 10 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.count()|0.001|0.001|0.001|0.001|5.687|
|RDD.distinct()|0.002|0.003|0.039|0.006|15.546|
|RDD.filter()|0.004|0.01|0.105|0.006|6.375|
|RDD.flatMap()|0.008|0.014|0.139|0.012|6.8|
|RDD.map()|0.005|0.002|0.04|0.003|6.799|
|RDD.map().reduce()|0.01|0.007|0.064|0.005|5.371|
|RDD.sortBy()|0.012|0.016|0.051|0.017|15.37|
|RDD.take()|0.001|0.006|0.037|0.003|7.141|
|RDD.union()|0.002|0.005|0.112|0.005|8.707|
|RDD.zip()|0.004|0.01|0.201|0.005|8.269|
|RDD.zipWithIndex()|0.035|0.057|0.076|0.023|6.492|

## DataSet

### Sample size = 100 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.count()|0.0|0.0|0.0|0.0|25.474|
|DataSet.distinct()|0.018|0.028|0.128|0.026|454.165|
|DataSet.filter()|0.004|0.032|0.349|0.013|15.04|
|DataSet.flatMap()|4.022|3.961|4.44|4.889|23.091|
|DataSet.groupByKey().count()|0.739|0.777|2.0|0.932|494.977|
|DataSet.groupByKey().mapGroups()|6.164|7.104|6.746|5.738|512.807|
|DataSet.limit()|0.001|0.002|0.022|0.004|2.349|
|DataSet.map()|0.811|0.516|1.052|0.615|17.875|
|DataSet.map().reduce()|0.717|0.664|1.037|0.707|14.303|
|DataSet.take()|0.001|0.002|0.027|0.002|3.178|
|DataSet.union()|3.821|3.813|4.393|3.907|19.479|

### Sample size = 1000 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.count()|0.0|0.0|0.0|0.0|30.127|
|DataSet.distinct()|0.195|0.281|0.251|0.282|634.981|
|DataSet.filter()|0.036|0.102|0.172|0.035|22.757|
|DataSet.flatMap()|4.341|4.45|4.607|4.204|35.745|
|DataSet.groupByKey().count()|1.203|1.67|1.828|1.253|719.659|
|DataSet.groupByKey().mapGroups()|5.919|6.176|6.854|6.137|696.82|
|DataSet.limit()|0.0|0.003|0.031|0.003|7.467|
|DataSet.map()|0.767|0.706|1.007|0.766|25.751|
|DataSet.map().reduce()|1.099|0.699|1.156|1.045|20.572|
|DataSet.take()|0.0|0.012|0.03|0.002|7.028|
|DataSet.union()|3.771|3.902|4.436|4.184|33.593|

### Sample size = 10 elements

|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.count()|0.001|0.002|0.002|0.002|72.429|
|DataSet.distinct()|0.011|0.022|0.501|0.033|437.562|
|DataSet.filter()|0.002|0.018|0.443|0.033|17.91|
|DataSet.flatMap()|9.793|5.947|5.693|4.638|24.236|
|DataSet.groupByKey().count()|0.87|0.851|2.32|0.811|465.308|
|DataSet.groupByKey().mapGroups()|7.063|7.368|8.447|6.782|481.68|
|DataSet.limit()|0.001|0.015|0.157|0.017|3.144|
|DataSet.map()|0.722|0.712|1.591|1.241|18.532|
|DataSet.map().reduce()|0.701|0.723|2.002|0.886|16.289|
|DataSet.take()|0.001|0.02|0.15|0.007|2.188|
|DataSet.union()|5.354|4.425|5.198|4.059|20.652|

# Images

## RDD

### Sample size = 100 elements

![](RDD_filter_100.png)

![](RDD_take_100.png)

![](RDD_distinct_100.png)

![](RDD_zip_100.png)

![](RDD_map_100.png)

![](RDD_sortBy_100.png)

![](RDD_zipWithIndex_100.png)

![](RDD_union_100.png)

![](RDD_count_100.png)

![](RDD_map().reduce_100.png)

![](RDD_flatMap_100.png)


### Sample size = 1000 elements

![](RDD_zipWithIndex_1000.png)

![](RDD_zip_1000.png)

![](RDD_map().reduce_1000.png)

![](RDD_map_1000.png)

![](RDD_count_1000.png)

![](RDD_take_1000.png)

![](RDD_union_1000.png)

![](RDD_filter_1000.png)

![](RDD_distinct_1000.png)

![](RDD_flatMap_1000.png)

![](RDD_sortBy_1000.png)


### Sample size = 10 elements

![](RDD_sortBy_10.png)

![](RDD_zip_10.png)

![](RDD_union_10.png)

![](RDD_zipWithIndex_10.png)

![](RDD_flatMap_10.png)

![](RDD_take_10.png)

![](RDD_count_10.png)

![](RDD_filter_10.png)

![](RDD_map_10.png)

![](RDD_map().reduce_10.png)

![](RDD_distinct_10.png)


## DataSet

### Sample size = 100 elements

![](DataSet_groupByKey().count_100.png)

![](DataSet_map_100.png)

![](DataSet_map().reduce_100.png)

![](DataSet_distinct_100.png)

![](DataSet_groupByKey().mapGroups_100.png)

![](DataSet_count_100.png)

![](DataSet_limit_100.png)

![](DataSet_flatMap_100.png)

![](DataSet_union_100.png)

![](DataSet_take_100.png)

![](DataSet_filter_100.png)


### Sample size = 1000 elements

![](DataSet_groupByKey().count_1000.png)

![](DataSet_take_1000.png)

![](DataSet_count_1000.png)

![](DataSet_map().reduce_1000.png)

![](DataSet_flatMap_1000.png)

![](DataSet_filter_1000.png)

![](DataSet_groupByKey().mapGroups_1000.png)

![](DataSet_limit_1000.png)

![](DataSet_distinct_1000.png)

![](DataSet_map_1000.png)

![](DataSet_union_1000.png)


### Sample size = 10 elements

![](DataSet_map().reduce_10.png)

![](DataSet_count_10.png)

![](DataSet_union_10.png)

![](DataSet_filter_10.png)

![](DataSet_groupByKey().count_10.png)

![](DataSet_flatMap_10.png)

![](DataSet_limit_10.png)

![](DataSet_take_10.png)

![](DataSet_map_10.png)

![](DataSet_groupByKey().mapGroups_10.png)

![](DataSet_distinct_10.png)

