# Benchmark results

This file summarizes benchmarks of different implementations.

# Summary

## Sample size = 100 elements

![](summary_100.png)

## Sample size = 1000 elements

![](summary_1000.png)

## Sample size = 10 elements

![](summary_10.png)

## Sample size = 100000 elements

![](summary_100000.png)

# Raw data

## RDD

### Sample size = 10 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.take()|0.001<br/>(0)%|0.001<br/>(0)%|0.031<br/>(0)%|0.002<br/>(0)%|7.373<br/>(100)%|
|RDD.zipWithIndex()|0.04<br/>(1)%|0.053<br/>(1)%|0.058<br/>(1)%|0.018<br/>(0)%|6.738<br/>(100)%|
|RDD.zip()|0.007<br/>(0)%|0.012<br/>(0)%|0.137<br/>(1)%|0.007<br/>(0)%|10.288<br/>(100)%|
|RDD.filter()|0.004<br/>(0)%|0.003<br/>(0)%|0.075<br/>(1)%|0.007<br/>(0)%|7.615<br/>(100)%|
|RDD.count()|0.001<br/>(0)%|0.001<br/>(0)%|0.002<br/>(0)%|0.002<br/>(0)%|7.934<br/>(100)%|
|RDD.union()|0.003<br/>(0)%|0.004<br/>(0)%|0.153<br/>(2)%|0.01<br/>(0)%|8.366<br/>(100)%|
|RDD.flatMap()|0.012<br/>(0)%|0.014<br/>(0)%|0.109<br/>(2)%|0.012<br/>(0)%|7.013<br/>(100)%|
|RDD.map()|0.005<br/>(0)%|0.001<br/>(0)%|0.077<br/>(1)%|0.003<br/>(0)%|7.039<br/>(100)%|
|RDD.distinct()|0.005<br/>(0)%|0.008<br/>(0)%|0.04<br/>(0)%|0.004<br/>(0)%|17.946<br/>(100)%|
|RDD.sortBy()|0.011<br/>(0)%|0.015<br/>(0)%|0.046<br/>(0)%|0.005<br/>(0)%|14.548<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.map().map().map()|0.013<br/>(0)%|0.003<br/>(0)%|0.169<br/>(2)%|0.004<br/>(0)%|7.844<br/>(100)%|
|RDD.map().reduce()|0.01<br/>(0)%|0.005<br/>(0)%|0.051<br/>(1)%|0.005<br/>(0)%|5.731<br/>(100)%|

### Sample size = 100 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.sortBy()|0.062<br/>(0)%|0.065<br/>(0)%|0.083<br/>(0)%|0.065<br/>(0)%|16.889<br/>(100)%|
|RDD.union()|0.01<br/>(0)%|0.03<br/>(0)%|0.104<br/>(1)%|0.018<br/>(0)%|9.765<br/>(100)%|
|RDD.flatMap()|0.043<br/>(1)%|0.045<br/>(1)%|0.139<br/>(2)%|0.023<br/>(0)%|7.397<br/>(100)%|
|RDD.count()|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|5.356<br/>(100)%|
|RDD.distinct()|0.014<br/>(0)%|0.023<br/>(0)%|0.071<br/>(0)%|0.023<br/>(0)%|18.528<br/>(100)%|
|RDD.filter()|0.004<br/>(0)%|0.01<br/>(0)%|0.123<br/>(2)%|0.004<br/>(0)%|7.284<br/>(100)%|
|RDD.take()|0.0<br/>(0)%|0.001<br/>(0)%|0.054<br/>(1)%|0.001<br/>(0)%|7.026<br/>(100)%|
|RDD.zipWithIndex()|0.05<br/>(1)%|0.082<br/>(1)%|0.085<br/>(1)%|0.016<br/>(0)%|6.97<br/>(100)%|
|RDD.map()|0.006<br/>(0)%|0.004<br/>(0)%|0.112<br/>(1)%|0.009<br/>(0)%|7.73<br/>(100)%|
|RDD.zip()|0.012<br/>(0)%|0.025<br/>(0)%|0.144<br/>(2)%|0.022<br/>(0)%|8.452<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.map().map().map()|0.025<br/>(0)%|0.015<br/>(0)%|0.229<br/>(3)%|0.017<br/>(0)%|7.986<br/>(100)%|
|RDD.map().reduce()|0.019<br/>(0)%|0.005<br/>(0)%|0.158<br/>(3)%|0.011<br/>(0)%|6.238<br/>(100)%|

### Sample size = 1000 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.distinct()|0.191<br/>(1)%|0.279<br/>(1)%|0.598<br/>(2)%|0.351<br/>(1)%|32.64<br/>(100)%|
|RDD.map()|0.05<br/>(1)%|0.035<br/>(0)%|0.154<br/>(2)%|0.044<br/>(1)%|8.437<br/>(100)%|
|RDD.flatMap()|0.597<br/>(6)%|0.526<br/>(5)%|0.358<br/>(3)%|0.201<br/>(2)%|10.527<br/>(100)%|
|RDD.zipWithIndex()|0.08<br/>(1)%|0.085<br/>(1)%|0.169<br/>(1)%|0.078<br/>(1)%|11.643<br/>(100)%|
|RDD.union()|0.092<br/>(1)%|0.164<br/>(1)%|0.451<br/>(3)%|0.233<br/>(2)%|13.423<br/>(100)%|
|RDD.sortBy()|0.65<br/>(2)%|0.463<br/>(2)%|0.384<br/>(1)%|0.4<br/>(1)%|30.83<br/>(100)%|
|RDD.count()|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|6.376<br/>(100)%|
|RDD.zip()|0.053<br/>(0)%|0.056<br/>(0)%|0.251<br/>(2)%|0.108<br/>(1)%|11.551<br/>(100)%|
|RDD.take()|0.0<br/>(0)%|0.002<br/>(0)%|0.034<br/>(0)%|0.001<br/>(0)%|7.768<br/>(100)%|
|RDD.filter()|0.036<br/>(0)%|0.043<br/>(0)%|0.17<br/>(2)%|0.04<br/>(0)%|8.794<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.map().map().map()|0.129<br/>(1)%|0.086<br/>(1)%|0.403<br/>(4)%|0.1<br/>(1)%|9.081<br/>(100)%|
|RDD.map().reduce()|0.061<br/>(1)%|0.046<br/>(1)%|0.205<br/>(3)%|0.058<br/>(1)%|7.128<br/>(100)%|

### Sample size = 100000 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.flatMap()|51.975<br/>(12)%|67.87<br/>(15)%|23.28<br/>(5)%|35.565<br/>(8)%|438.691<br/>(100)%|
|RDD.sortBy()|45.564<br/>(6)%|58.688<br/>(7)%|39.826<br/>(5)%|52.423<br/>(7)%|784.586<br/>(100)%|
|RDD.count()|0.001<br/>(0)%|0.002<br/>(0)%|0.002<br/>(0)%|0.002<br/>(0)%|173.631<br/>(100)%|
|RDD.take()|0.003<br/>(0)%|0.013<br/>(0)%|0.096<br/>(0)%|0.032<br/>(0)%|161.786<br/>(100)%|
|RDD.map()|6.701<br/>(4)%|6.908<br/>(4)%|7.99<br/>(4)%|8.406<br/>(5)%|182.35<br/>(100)%|
|RDD.union()|7.788<br/>(1)%|15.692<br/>(2)%|5.958<br/>(1)%|17.147<br/>(3)%|655.888<br/>(100)%|
|RDD.zip()|8.227<br/>(2)%|3.444<br/>(1)%|7.148<br/>(1)%|4.961<br/>(1)%|518.214<br/>(100)%|
|RDD.zipWithIndex()|6.209<br/>(1)%|7.164<br/>(1)%|9.547<br/>(2)%|9.323<br/>(2)%|525.995<br/>(100)%|
|RDD.filter()|5.865<br/>(2)%|9.471<br/>(4)%|5.016<br/>(2)%|10.531<br/>(4)%|265.274<br/>(100)%|
|RDD.distinct()|28.212<br/>(3)%|40.114<br/>(4)%|27.999<br/>(3)%|37.155<br/>(4)%|1061.363<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|RDD.map().reduce()|6.189<br/>(4)%|7.717<br/>(5)%|5.356<br/>(3)%|8.228<br/>(5)%|170.874<br/>(100)%|
|RDD.map().map().map()|10.047<br/>(5)%|10.536<br/>(6)%|10.657<br/>(6)%|16.538<br/>(9)%|185.275<br/>(100)%|

## DataSet

### Sample size = 10 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.filter()|0.004<br/>(0)%|0.015<br/>(0)%|0.469<br/>(2)%|0.018<br/>(0)%|18.898<br/>(100)%|
|DataSet.distinct()|0.015<br/>(0)%|0.024<br/>(0)%|0.452<br/>(0)%|0.039<br/>(0)%|476.069<br/>(100)%|
|DataSet.limit()|0.002<br/>(0)%|0.011<br/>(0)%|0.091<br/>(3)%|0.019<br/>(1)%|3.481<br/>(100)%|
|DataSet.count()|0.001<br/>(0)%|0.002<br/>(0)%|0.001<br/>(0)%|0.002<br/>(0)%|47.053<br/>(100)%|
|DataSet.flatMap()|9.802<br/>(37)%|6.449<br/>(24)%|5.639<br/>(21)%|4.588<br/>(17)%|26.553<br/>(100)%|
|DataSet.union()|7.135<br/>(37)%|8.152<br/>(42)%|7.039<br/>(37)%|4.395<br/>(23)%|19.19<br/>(100)%|
|DataSet.take()|0.002<br/>(0)%|0.038<br/>(2)%|0.077<br/>(3)%|0.01<br/>(0)%|2.307<br/>(100)%|
|DataSet.map()|0.843<br/>(5)%|0.656<br/>(4)%|1.251<br/>(7)%|0.714<br/>(4)%|16.749<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.groupByKey().mapGroups()|6.596<br/>(1)%|8.027<br/>(2)%|8.368<br/>(2)%|5.804<br/>(1)%|488.057<br/>(100)%|
|DataSet.groupByKey().count()|0.799<br/>(0)%|0.959<br/>(0)%|2.09<br/>(0)%|0.829<br/>(0)%|480.008<br/>(100)%|
|DataSet.map().map().map()|1.931<br/>(7)%|2.216<br/>(8)%|3.753<br/>(14)%|3.395<br/>(13)%|26.624<br/>(100)%|
|DataSet.map().filter().groupByKey().mapGroups()|9.941<br/>(2)%|9.123<br/>(2)%|9.601<br/>(2)%|10.345<br/>(2)%|511.317<br/>(100)%|
|DataSet.map().reduce()|1.041<br/>(5)%|1.149<br/>(6)%|1.094<br/>(5)%|0.768<br/>(4)%|20.801<br/>(100)%|

### Sample size = 100 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.limit()|0.001<br/>(0)%|0.003<br/>(0)%|0.041<br/>(1)%|0.007<br/>(0)%|3.132<br/>(100)%|
|DataSet.union()|3.732<br/>(20)%|3.779<br/>(20)%|4.367<br/>(23)%|3.846<br/>(20)%|19.089<br/>(100)%|
|DataSet.map()|0.639<br/>(4)%|0.645<br/>(4)%|1.084<br/>(7)%|0.721<br/>(5)%|15.565<br/>(100)%|
|DataSet.flatMap()|3.776<br/>(17)%|3.742<br/>(17)%|4.218<br/>(19)%|3.839<br/>(17)%|22.393<br/>(100)%|
|DataSet.count()|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|34.628<br/>(100)%|
|DataSet.distinct()|0.028<br/>(0)%|0.044<br/>(0)%|0.252<br/>(0)%|0.05<br/>(0)%|452.536<br/>(100)%|
|DataSet.take()|0.002<br/>(0)%|0.003<br/>(0)%|0.036<br/>(2)%|0.005<br/>(0)%|2.224<br/>(100)%|
|DataSet.filter()|0.004<br/>(0)%|0.031<br/>(0)%|0.189<br/>(1)%|0.008<br/>(0)%|15.717<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map().reduce()|0.641<br/>(5)%|0.643<br/>(5)%|0.993<br/>(7)%|0.713<br/>(5)%|13.432<br/>(100)%|
|DataSet.groupByKey().count()|0.752<br/>(0)%|0.726<br/>(0)%|1.732<br/>(0)%|0.821<br/>(0)%|496.707<br/>(100)%|
|DataSet.groupByKey().mapGroups()|5.277<br/>(1)%|4.921<br/>(1)%|5.936<br/>(1)%|4.803<br/>(1)%|521.733<br/>(100)%|
|DataSet.map().map().map()|1.892<br/>(8)%|1.909<br/>(8)%|2.506<br/>(11)%|1.906<br/>(8)%|23.006<br/>(100)%|
|DataSet.map().filter().groupByKey().mapGroups()|9.149<br/>(2)%|8.309<br/>(2)%|9.435<br/>(2)%|9.643<br/>(2)%|528.935<br/>(100)%|

### Sample size = 1000 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map()|0.686<br/>(3)%|0.67<br/>(3)%|0.939<br/>(4)%|0.915<br/>(4)%|23.167<br/>(100)%|
|DataSet.count()|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|0.0<br/>(0)%|30.962<br/>(100)%|
|DataSet.distinct()|0.23<br/>(0)%|0.296<br/>(0)%|0.258<br/>(0)%|0.362<br/>(0)%|653.526<br/>(100)%|
|DataSet.filter()|0.037<br/>(0)%|0.133<br/>(1)%|0.132<br/>(1)%|0.042<br/>(0)%|24.553<br/>(100)%|
|DataSet.flatMap()|4.5<br/>(13)%|5.613<br/>(16)%|5.35<br/>(15)%|4.373<br/>(12)%|35.646<br/>(100)%|
|DataSet.limit()|0.001<br/>(0)%|0.001<br/>(0)%|0.031<br/>(0)%|0.002<br/>(0)%|7.569<br/>(100)%|
|DataSet.take()|0.001<br/>(0)%|0.002<br/>(0)%|0.029<br/>(0)%|0.002<br/>(0)%|6.861<br/>(100)%|
|DataSet.union()|4.282<br/>(14)%|3.756<br/>(12)%|4.174<br/>(13)%|3.792<br/>(12)%|31.302<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map().reduce()|0.69<br/>(3)%|0.673<br/>(3)%|1.529<br/>(7)%|0.673<br/>(3)%|20.746<br/>(100)%|
|DataSet.groupByKey().mapGroups()|6.104<br/>(1)%|5.811<br/>(1)%|5.954<br/>(1)%|6.715<br/>(1)%|730.138<br/>(100)%|
|DataSet.map().filter().groupByKey().mapGroups()|9.755<br/>(1)%|8.542<br/>(1)%|10.42<br/>(1)%|8.785<br/>(1)%|743.158<br/>(100)%|
|DataSet.map().map().map()|2.153<br/>(7)%|1.989<br/>(6)%|2.671<br/>(9)%|2.04<br/>(7)%|30.818<br/>(100)%|
|DataSet.groupByKey().count()|1.347<br/>(0)%|1.387<br/>(0)%|1.454<br/>(0)%|1.297<br/>(0)%|675.576<br/>(100)%|

### Sample size = 100000 elements


#### Operations: basic
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map()|7.434<br/>(1)%|9.346<br/>(1)%|14.56<br/>(1)%|9.342<br/>(1)%|1081.195<br/>(100)%|
|DataSet.union()|12.838<br/>(1)%|21.828<br/>(1)%|13.354<br/>(1)%|22.727<br/>(1)%|1809.351<br/>(100)%|
|DataSet.filter()|5.009<br/>(0)%|11.68<br/>(1)%|9.443<br/>(1)%|6.254<br/>(1)%|1108.817<br/>(100)%|
|DataSet.flatMap()|77.111<br/>(4)%|103.853<br/>(5)%|44.539<br/>(2)%|46.419<br/>(2)%|2041.143<br/>(100)%|
|DataSet.count()|0.002<br/>(0)%|0.003<br/>(0)%|0.002<br/>(0)%|0.002<br/>(0)%|895.142<br/>(100)%|
|DataSet.distinct()|27.881<br/>(0)%|28.548<br/>(0)%|29.277<br/>(0)%|30.373<br/>(0)%|16020.734<br/>(100)%|
|DataSet.take()|0.003<br/>(0)%|0.013<br/>(0)%|0.089<br/>(0)%|0.019<br/>(0)%|675.497<br/>(100)%|
|DataSet.limit()|0.005<br/>(0)%|0.024<br/>(0)%|0.176<br/>(0)%|0.038<br/>(0)%|662.274<br/>(100)%|

#### Operations: complex
|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|
|--|--|--|--|--|--|
|DataSet.map().map().map()|13.468<br/>(1)%|13.702<br/>(1)%|16.632<br/>(1)%|16.626<br/>(1)%|1546.064<br/>(100)%|
|DataSet.groupByKey().mapGroups()|199.618<br/>(1)%|199.097<br/>(1)%|92.222<br/>(1)%|205.046<br/>(1)%|15556.168<br/>(100)%|
|DataSet.map().filter().groupByKey().mapGroups()|75.209<br/>(0)%|68.474<br/>(0)%|48.556<br/>(0)%|77.966<br/>(1)%|15585.99<br/>(100)%|
|DataSet.groupByKey().count()|80.221<br/>(1)%|89.514<br/>(1)%|62.506<br/>(0)%|98.563<br/>(1)%|15303.637<br/>(100)%|
|DataSet.map().reduce()|6.884<br/>(1)%|9.26<br/>(1)%|7.706<br/>(1)%|10.365<br/>(1)%|927.849<br/>(100)%|

# Comparing Scala implementations

## RDD

### Sample size = 10 elements


#### Operations: basic
![](RDD_filter_10.png)

![](RDD_union_10.png)

![](RDD_map_10.png)

![](RDD_zip_10.png)

![](RDD_zipWithIndex_10.png)

![](RDD_flatMap_10.png)

![](RDD_sortBy_10.png)

![](RDD_take_10.png)

![](RDD_count_10.png)

![](RDD_distinct_10.png)


#### Operations: complex
![](RDD_map().reduce_10.png)

![](RDD_map().map().map_10.png)


### Sample size = 100 elements


#### Operations: basic
![](RDD_union_100.png)

![](RDD_zip_100.png)

![](RDD_flatMap_100.png)

![](RDD_filter_100.png)

![](RDD_distinct_100.png)

![](RDD_take_100.png)

![](RDD_count_100.png)

![](RDD_zipWithIndex_100.png)

![](RDD_sortBy_100.png)

![](RDD_map_100.png)


#### Operations: complex
![](RDD_map().map().map_100.png)

![](RDD_map().reduce_100.png)


### Sample size = 1000 elements


#### Operations: basic
![](RDD_distinct_1000.png)

![](RDD_zipWithIndex_1000.png)

![](RDD_zip_1000.png)

![](RDD_count_1000.png)

![](RDD_take_1000.png)

![](RDD_filter_1000.png)

![](RDD_sortBy_1000.png)

![](RDD_flatMap_1000.png)

![](RDD_map_1000.png)

![](RDD_union_1000.png)


#### Operations: complex
![](RDD_map().reduce_1000.png)

![](RDD_map().map().map_1000.png)


### Sample size = 100000 elements


#### Operations: basic
![](RDD_map_100000.png)

![](RDD_union_100000.png)

![](RDD_filter_100000.png)

![](RDD_sortBy_100000.png)

![](RDD_flatMap_100000.png)

![](RDD_zipWithIndex_100000.png)

![](RDD_distinct_100000.png)

![](RDD_zip_100000.png)

![](RDD_count_100000.png)

![](RDD_take_100000.png)


#### Operations: complex
![](RDD_map().map().map_100000.png)

![](RDD_map().reduce_100000.png)


## DataSet

### Sample size = 10 elements


#### Operations: basic
![](DataSet_flatMap_10.png)

![](DataSet_distinct_10.png)

![](DataSet_limit_10.png)

![](DataSet_count_10.png)

![](DataSet_filter_10.png)

![](DataSet_map_10.png)

![](DataSet_union_10.png)

![](DataSet_take_10.png)


#### Operations: complex
![](DataSet_groupByKey().count_10.png)

![](DataSet_groupByKey().mapGroups_10.png)

![](DataSet_map().reduce_10.png)

![](DataSet_map().map().map_10.png)

![](DataSet_map().filter().groupByKey().mapGroups_10.png)


### Sample size = 100 elements


#### Operations: basic
![](DataSet_flatMap_100.png)

![](DataSet_limit_100.png)

![](DataSet_take_100.png)

![](DataSet_union_100.png)

![](DataSet_map_100.png)

![](DataSet_distinct_100.png)

![](DataSet_filter_100.png)

![](DataSet_count_100.png)


#### Operations: complex
![](DataSet_groupByKey().count_100.png)

![](DataSet_map().reduce_100.png)

![](DataSet_map().map().map_100.png)

![](DataSet_map().filter().groupByKey().mapGroups_100.png)

![](DataSet_groupByKey().mapGroups_100.png)


### Sample size = 1000 elements


#### Operations: basic
![](DataSet_flatMap_1000.png)

![](DataSet_limit_1000.png)

![](DataSet_filter_1000.png)

![](DataSet_map_1000.png)

![](DataSet_count_1000.png)

![](DataSet_distinct_1000.png)

![](DataSet_take_1000.png)

![](DataSet_union_1000.png)


#### Operations: complex
![](DataSet_map().filter().groupByKey().mapGroups_1000.png)

![](DataSet_groupByKey().mapGroups_1000.png)

![](DataSet_map().reduce_1000.png)

![](DataSet_map().map().map_1000.png)

![](DataSet_groupByKey().count_1000.png)


### Sample size = 100000 elements


#### Operations: basic
![](DataSet_count_100000.png)

![](DataSet_limit_100000.png)

![](DataSet_flatMap_100000.png)

![](DataSet_take_100000.png)

![](DataSet_distinct_100000.png)

![](DataSet_map_100000.png)

![](DataSet_union_100000.png)

![](DataSet_filter_100000.png)


#### Operations: complex
![](DataSet_map().filter().groupByKey().mapGroups_100000.png)

![](DataSet_groupByKey().count_100000.png)

![](DataSet_map().map().map_100000.png)

![](DataSet_map().reduce_100000.png)

![](DataSet_groupByKey().mapGroups_100000.png)

