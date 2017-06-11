# Benchmark results

# Raw data

## Sample size = 10 elements
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map()|4.607|3.29|3.466|3.078|40.56|
|DataSet.count()|0.001|0.001|0.001|0.001|37.85|
|DataSet.filter()|0.005|0.044|0.389|0.024|22.859|
|DataSet.union()|6.712|5.104|6.154|4.583|25.792|
|DataSet.flatMap()|5.771|6.124|6.679|4.317|27.656|
|DataSet.limit()|0.001|0.008|0.056|0.013|4.587|
|DataSet.take()|0.002|0.008|0.076|0.01|3.291|
|DataSet.distinct()|0.006|0.009|0.077|0.022|502.497|
|DataSet.map().reduce()|0.745|0.778|1.221|0.858|17.468|
|RDD.map()|0.005|0.005|0.283|0.007|9.071|
|RDD.count()|0.0|0.001|0.001|0.0|6.973|
|RDD.filter()|0.003|0.014|0.154|0.005|9.106|
|RDD.union()|0.003|0.008|0.193|0.015|10.152|
|RDD.flatMap()|0.019|0.025|0.132|0.01|9.169|
|RDD.take()|0.002|0.006|0.036|0.009|10.364|
|RDD.distinct()|0.004|0.106|0.076|0.01|22.391|
|RDD.map().reduce()|0.008|0.005|0.148|0.008|9.657|

# Images

## RDD
![](benchmarks/RDD_union_10.png)

![](benchmarks/RDD_flatMap_10.png)

![](benchmarks/RDD_take_10.png)

![](benchmarks/RDD_count_10.png)

![](benchmarks/RDD_filter_10.png)

![](benchmarks/RDD_map_10.png)

![](benchmarks/RDD_map().reduce_10.png)

![](benchmarks/RDD_distinct_10.png)


## DataSet
![](benchmarks/DataSet_map().reduce_10.png)

![](benchmarks/DataSet_count_10.png)

![](benchmarks/DataSet_union_10.png)

![](benchmarks/DataSet_filter_10.png)

![](benchmarks/DataSet_flatMap_10.png)

![](benchmarks/DataSet_limit_10.png)

![](benchmarks/DataSet_take_10.png)

![](benchmarks/DataSet_map_10.png)

![](benchmarks/DataSet_distinct_10.png)

