# Supported Spark operations

## SparkContext

|Operation|Supported?|
|---------|---------|
|addFile||
|addJar||
|addSparkListener||
|broadcast|![](images/API-supported-green.png)|
|cancelAllJobs||
|cancelJob||
|cancelJobGroup||
|cancelStage||
|clearCallSite||
|clearJobGroup||
|collectionAccumulator|![](images/API-supported-green.png)|
|defaultMinPartitions||
|defaultParallelism||
|deployMode||
|doubleAccumulator|![](images/API-supported-green.png)|
|emptyRDD||
|files||
|getAllPools||
|getCheckpointDir||
|getConf||
|getExecutorMemoryStatus||
|getExecutorStorageStatus||
|getLocalProperty||
|getPersistentRDDs||
|getPoolForName||
|getRDDStorageInfo||
|getSchedulingMode||
|hadoopConfiguration||
|hadoopFile||
|hadoopRDD||
|isLocal||
|isStopped||
|jars||
|killExecutor||
|killExecutors||
|listFiles||
|listJars||
|longAccumulator|![](images/API-supported-green.png)|
|makeRDD||
|master||
|newAPIHadoopFile||
|newAPIHadoopRDD||
|objectFile||
|parallelize||
|range||
|register|![](images/API-supported-green.png)|
|requestExecutors||
|requestTotalExecutors||
|runApproximateJob||
|runJob||
|sequenceFile||
|setCallSite||
|setCheckpointDir||
|setJobDescription||
|setJobGroup||
|setLocalProperty||
|setLogLevel||
|sparkUser||
|startTime||
|statusTracker||
|stop||
|submitJob||
|textFile|![](images/API-supported-green.png)|
|uiWebUr||
|union||
|version||
|wholeTextFiles||

## RDD API

### RDD API - basic actions and transformations

|Operation|Supported?|
|---------|---------|
|aggregate||
|cache|![](images/API-supported-green.png)|
|cartesian|![](images/API-supported-green.png)|
|checkpoint|![](images/API-supported-green.png)|
|coalesce|![](images/API-supported-green.png)|
|collect| ![](images/API-supported-green.png)|
|count| ![](images/API-supported-green.png)|
|countApprox||
|countApproxDistinct||
|countByValue||
|countByValueApprox||
|dependencies||
|distinct|![](images/API-supported-green.png)|
|filter| ![](images/API-supported-green.png)|
|first|![](images/API-supported-green.png)|
|flatMap| ![](images/API-supported-green.png)|
|fold| ![](images/API-supported-green.png)|
|foreach| ![](images/API-supported-green.png)|
|foreachPartition| ![](images/API-supported-green.png)|
|getCheckpointFile||
|getNumPartitions||
|getStorageLevel||
|glom||
|groupBy|![](images/API-supported-green.png)|
|id||
|intersection| ![](images/API-supported-green.png)|
|isCheckpointed||
|isEmpty| ![](images/API-supported-green.png)|
|iterator||
|keyBy|![](images/API-supported-green.png)|
|localCheckpoint||
|map| ![](images/API-supported-green.png)|
|mapPartitions| ![](images/API-supported-green.png)|
|mapPartitionsWithIndex||
|max| ![](images/API-supported-green.png)|
|min| ![](images/API-supported-green.png)|
|name||
|partitioner||
|partitions||
|persist| ![](images/API-supported-green.png)|
|pipe||
|preferredLocations||
|randomSplit|![](images/API-supported-green.png)|
|reduce| ![](images/API-supported-green.png)|
|repartition|![](images/API-supported-green.png)|
|sample|![](images/API-supported-green.png)|
|saveAsObjectFile||
|saveAsTextFile||
|setName||
|sortBy| ![](images/API-supported-green.png)|
|subtract|![](images/API-supported-green.png)|
|take| ![](images/API-supported-green.png)|
|takeOrdered|![](images/API-supported-green.png)|
|takeSample|![](images/API-supported-green.png)|
|toDebugString||
|toJavaRDD||
|toLocalIterator||
|top|![](images/API-supported-green.png)|
|treeAggregate||
|treeReduce||
|union| ![](images/API-supported-green.png)|
|unpersist|![](images/API-supported-green.png)|
|zip| ![](images/API-supported-green.png)|
|zipPartitions||
|zipWithIndex| ![](images/API-supported-green.png)|
|zipWithUniqueId||

### Pair RDD API

|Operation|Supported?|
|---------|---------|
|aggregateByKey|![](images/API-supported-green.png)|
|cogroup|![](images/API-supported-green.png)|
|collectAsMap|![](images/API-supported-green.png)|
|combineByKey||
|combineByKeyWithClassTag||
|countApproxDistinctByKey||
|countByKey|![](images/API-supported-green.png)|
|countByKeyApprox||
|flatMapValues|![](images/API-supported-green.png)|
|foldByKey|![](images/API-supported-green.png)|
|fullOuterJoin|![](images/API-supported-green.png)|
|groupByKey|![](images/API-supported-green.png)|
|groupWith||
|join|![](images/API-supported-green.png)|
|keys|![](images/API-supported-green.png)|
|leftOuterJoin|![](images/API-supported-green.png)|
|lookup||
|mapValues|![](images/API-supported-green.png)|
|partitionBy|![](images/API-supported-green.png)|
|reduceByKey|![](images/API-supported-green.png)|
|reduceByKeyLocally|![](images/API-supported-green.png)|
|rightOuterJoin|![](images/API-supported-green.png)|
|sampleByKey||
|sampleByKeyExact||
|saveAsHadoopDataset||
|saveAsHadoopFile||
|saveAsNewAPIHadoopDataset||
|subtractByKey|![](images/API-supported-green.png)|
|values|![](images/API-supported-green.png)|

## Dataset API

### Dataset API - Actions

|Operation|Supported?|
|---------|---------|
|collect| ![](images/API-supported-green.png)|
|collectAsList|![](images/API-supported-green.png)|
|count| ![](images/API-supported-green.png)|
|describe||
|first| ![](images/API-supported-green.png)|
|foreach| ![](images/API-supported-green.png)|
|foreachPartition| ![](images/API-supported-green.png)|
|head| ![](images/API-supported-green.png)|
|reduce| ![](images/API-supported-green.png)|
|show||
|take| ![](images/API-supported-green.png)|
|takeAsList|![](images/API-supported-green.png)|
|toLocalIterator||

### Dataset API - Basic Dataset functions

|Operation|Supported?|
|---------|---------|
|as||
|cache| ![](images/API-supported-green.png)|
|checkpoint| ![](images/API-supported-green.png)|
|columns||
|createGlobalTempView||
|createOrReplaceTempView||
|createTempView||
|dtypes||
|explain||
|inputFiles||
|isLocal||
|javaRDD||
|persist| ![](images/API-supported-green.png)|
|printSchema||
|rdd|![](images/API-supported-green.png)|
|schema||
|storageLevel||
|toDF||
|toJavaRDD||
|unpersist|![](images/API-supported-green.png)|
|write||
|writeStream||

### Dataset API - streaming

|Operation|Supported?|
|---------|---------|
|isStreaming||
|withWatermark||

### Dataset API - Typed transformations

|Operation|Supported?|
|---------|---------|
|alias||
|as||
|coalesce||
|distinct|![](images/API-supported-green.png)|
|dropDuplicates||
|except|![](images/API-supported-green.png)|
|filter| ![](images/API-supported-green.png)|
|flatMap| ![](images/API-supported-green.png)|
|groupByKey|![](images/API-supported-green.png)|
|intersect|![](images/API-supported-green.png)|
|joinWith||
|limit|![](images/API-supported-green.png)|
|map|![](images/API-supported-green.png)|
|mapPartitions| ![](images/API-supported-green.png)|
|orderBy||
|randomSplit|![](images/API-supported-green.png)|
|randomSplitAsList||
|repartition||
|sample|![](images/API-supported-green.png)|
|select||
|sort||
|sortWithinPartitions||
|transform||
|union|![](images/API-supported-green.png)|
|where||

### Dataset API - Untyped transformations

|Operation|Supported?|
|---------|---------|
|agg||
|apply||
|col||
|crossJoin||
|cube||
|drop||
|groupBy||
|join||
|na||
|rollup||
|select||
|selectExpr||
|stat||
|withColumn||
|withColumnRenamed||

### KeyValueGroupedDataset API

|Operation|Supported?|
|---------|---------|
|agg||
|cogroup|![](images/API-supported-green.png)|
|count|![](images/API-supported-green.png)|
|flatMapGroups|![](images/API-supported-green.png)|
|keyAs||
|keys|![](images/API-supported-green.png)|
|mapGroups|![](images/API-supported-green.png)|
|mapValues|![](images/API-supported-green.png)|
|reduceGroups|![](images/API-supported-green.png)|

### Dataset - additional API
|Operation|Supported?|
|---------|---------|
|join|![](images/API-supported-green.png)|
|leftOuterJoin|![](images/API-supported-green.png)|
|rightOuterJoin|![](images/API-supported-green.png)|
|fullOuterJoin|![](images/API-supported-green.png)|