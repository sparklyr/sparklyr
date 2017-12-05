Distributing R Computations
================

Overview
--------

**sparklyr** provides support to run arbitrary R code at scale within your Spark Cluster through `spark_apply()`. This is especially useful where there is a need to use functionality available only in R or R packages that is not available in Apache Spark nor [Spark Packages](https://spark-packages.org/).

`spark_apply()` applies an R function to a Spark object (typically, a Spark DataFrame). Spark objects are partitioned so they can be distributed across a cluster. You can use `spark_apply` with the default partitions or you can define your own partitions with the `group_by` argument. Your R function must return another Spark DataFrame. `spark_apply` will run your R function on each partition and output a single Spark DataFrame.

### Apply an R function to a Spark Object

Lets run a simple example. We will apply the identify function, `I()`, over a list of numbers we created with the `sdf_len` function.

``` r
library(sparklyr)

sc <- spark_connect(master = "local")

sdf_len(sc, 5, repartition = 1) %>%
  spark_apply(function(e) I(e))
```

    ## # Source:   table<sparklyr_tmp_378c2e4fb50> [?? x 1]
    ## # Database: spark_connection
    ##      id
    ##   <dbl>
    ## 1     1
    ## 2     2
    ## 3     3
    ## 4     4
    ## 5     5

Your R function should be designed to operate on an R [data frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html). The R function passed to `spark_apply` expects a DataFrame and will return an object that can be cast as a DataFrame. We can use the `class` function to verify the class of the data.

``` r
sdf_len(sc, 10, repartition = 1) %>%
  spark_apply(function(e) class(e))
```

    ## # Source:   table<sparklyr_tmp_378c7ce7618d> [?? x 1]
    ## # Database: spark_connection
    ##           id
    ##        <chr>
    ## 1 data.frame

Spark will partition your data by hash or range so it can be distributed across a cluster. In the following example we create two partitions and count the number of rows in each partition. Then we print the first record in each partition.

``` r
trees_tbl <- sdf_copy_to(sc, trees, repartition = 2)

trees_tbl %>%
  spark_apply(function(e) nrow(e), names = "n")
```

    ## # Source:   table<sparklyr_tmp_378c15c45eb1> [?? x 1]
    ## # Database: spark_connection
    ##       n
    ##   <int>
    ## 1    16
    ## 2    15

``` r
trees_tbl %>%
  spark_apply(function(e) head(e, 1))
```

    ## # Source:   table<sparklyr_tmp_378c29215418> [?? x 3]
    ## # Database: spark_connection
    ##   Girth Height Volume
    ##   <dbl>  <dbl>  <dbl>
    ## 1   8.3     70   10.3
    ## 2   8.6     65   10.3

We can apply any arbitrary function to the partitions in the Spark DataFrame. For instance, we can scale or jitter the columns. Notice that `spark_apply` applies the R function to all partitions and returns a single DataFrame.

``` r
trees_tbl %>%
  spark_apply(function(e) scale(e))
```

    ## # Source:   table<sparklyr_tmp_378c8922ba8> [?? x 3]
    ## # Database: spark_connection
    ##         Girth      Height     Volume
    ##         <dbl>       <dbl>      <dbl>
    ##  1 -1.4482330 -0.99510521 -1.1503645
    ##  2 -1.3021313 -2.06675697 -1.1558670
    ##  3 -0.7469449  0.68891899 -0.6826528
    ##  4 -0.6592839 -1.60747764 -0.8587325
    ##  5 -0.6300635  0.53582588 -0.4735581
    ##  6 -0.5716229  0.38273277 -0.3855183
    ##  7 -0.5424025 -0.07654655 -0.5395880
    ##  8 -0.3670805 -0.22963966 -0.6661453
    ##  9 -0.1040975  1.30129143  0.1427209
    ## 10  0.1296653 -0.84201210 -0.3029809
    ## # ... with more rows

``` r
trees_tbl %>%
  spark_apply(function(e) lapply(e, jitter))
```

    ## # Source:   table<sparklyr_tmp_378c43237574> [?? x 3]
    ## # Database: spark_connection
    ##        Girth   Height   Volume
    ##        <dbl>    <dbl>    <dbl>
    ##  1  8.319392 70.04321 10.30556
    ##  2  8.801237 62.85795 10.21751
    ##  3 10.719805 81.15618 18.78076
    ##  4 11.009892 65.98926 15.58448
    ##  5 11.089322 80.14661 22.58749
    ##  6 11.309682 79.01360 24.18158
    ##  7 11.418486 75.88748 21.38380
    ##  8 11.982421 74.85612 19.09375
    ##  9 12.907616 84.81742 33.80591
    ## 10 13.691892 71.05309 25.70321
    ## # ... with more rows

By default `spark_apply()` derives the column names from the input Spark data frame. Use the `names` argument to rename or add new columns.

``` r
trees_tbl %>%
  spark_apply(
    function(e) data.frame(2.54 * e$Girth, e),
    names = c("Girth(cm)", colnames(trees)))
```

    ## # Source:   table<sparklyr_tmp_378c14e015b5> [?? x 4]
    ## # Database: spark_connection
    ##    `Girth(cm)` Girth Height Volume
    ##          <dbl> <dbl>  <dbl>  <dbl>
    ##  1      21.082   8.3     70   10.3
    ##  2      22.352   8.8     63   10.2
    ##  3      27.178  10.7     81   18.8
    ##  4      27.940  11.0     66   15.6
    ##  5      28.194  11.1     80   22.6
    ##  6      28.702  11.3     79   24.2
    ##  7      28.956  11.4     76   21.4
    ##  8      30.480  12.0     75   19.1
    ##  9      32.766  12.9     85   33.8
    ## 10      34.798  13.7     71   25.7
    ## # ... with more rows

### Group By

In some cases you may want to apply your R function to specific groups in your data. For example, suppose you want to compute regression models against specific subgroups. To solve this, you can specify a `group_by` argument. This example counts the number of rows in `iris` by species and then fits a simple linear model for each species.

``` r
iris_tbl <- sdf_copy_to(sc, iris)

iris_tbl %>%
  spark_apply(nrow, group_by = "Species")
```

    ## # Source:   table<sparklyr_tmp_378c1b8155f3> [?? x 2]
    ## # Database: spark_connection
    ##      Species Sepal_Length
    ##        <chr>        <int>
    ## 1 versicolor           50
    ## 2  virginica           50
    ## 3     setosa           50

``` r
iris_tbl %>%
  spark_apply(
    function(e) summary(lm(Petal_Length ~ Petal_Width, e))$r.squared,
    names = "r.squared",
    group_by = "Species")
```

    ## # Source:   table<sparklyr_tmp_378c30e6155> [?? x 2]
    ## # Database: spark_connection
    ##      Species r.squared
    ##        <chr>     <dbl>
    ## 1 versicolor 0.6188467
    ## 2  virginica 0.1037537
    ## 3     setosa 0.1099785

Distributing Packages
---------------------

With `spark_apply()` you can use any R package inside Spark. For instance, you can use the [broom](https://cran.r-project.org/package=broom) package to create a tidy data frame from linear regression output.

``` r
spark_apply(
  iris_tbl,
  function(e) broom::tidy(lm(Petal_Length ~ Petal_Width, e)),
  names = c("term", "estimate", "std.error", "statistic", "p.value"),
  group_by = "Species")
```

    ## # Source:   table<sparklyr_tmp_378c5502500b> [?? x 6]
    ## # Database: spark_connection
    ##      Species        term  estimate std.error statistic      p.value
    ##        <chr>       <chr>     <dbl>     <dbl>     <dbl>        <dbl>
    ## 1 versicolor (Intercept) 1.7812754 0.2838234  6.276000 9.484134e-08
    ## 2 versicolor Petal_Width 1.8693247 0.2117495  8.827999 1.271916e-11
    ## 3  virginica (Intercept) 4.2406526 0.5612870  7.555230 1.041600e-09
    ## 4  virginica Petal_Width 0.6472593 0.2745804  2.357267 2.253577e-02
    ## 5     setosa (Intercept) 1.3275634 0.0599594 22.141037 7.676120e-27
    ## 6     setosa Petal_Width 0.5464903 0.2243924  2.435422 1.863892e-02

To use R packages inside Spark, your packages must be installed on the worker nodes. The first time you call `spark_apply` all of the contents in your local `.libPaths()` will be copied into each Spark worker node via the `SparkConf.addFile()` function. Packages will only be copied once and will persist as long as the connection remains open. It's not uncommon for R libraries to be several gigabytes in size, so be prepared for a one-time tax while the R packages are copied over to your Spark cluster. You can disable package distribution by setting `packages = FALSE`. Note: packages are not copied in local mode (`master="local"`) because the packages already exist on the system.

Handling Errors
---------------

It can be more difficult to troubleshoot R issues in a cluster than in local mode. For instance, the following R code causes the distributed execution to fail and suggests you check the logs for details.

``` r
spark_apply(iris_tbl, function(e) stop("Make this fail"))
```

     Error in force(code) : 
      sparklyr worker rscript failure, check worker logs for details

In local mode, `sparklyr` will retrieve the logs for you. The logs point out the real failure as `ERROR sparklyr: RScript (4190) Make this fail` as you might expect.

    ---- Output Log ----
    (17/07/27 21:24:18 ERROR sparklyr: Worker (2427) is shutting down with exception ,java.net.SocketException: Socket closed)
    17/07/27 21:24:18 WARN TaskSetManager: Lost task 0.0 in stage 389.0 (TID 429, localhost, executor driver): 17/07/27 21:27:21 INFO sparklyr: RScript (4190) retrieved 150 rows 
    17/07/27 21:27:21 INFO sparklyr: RScript (4190) computing closure 
    17/07/27 21:27:21 ERROR sparklyr: RScript (4190) Make this fail 

It is worth mentioning that different cluster providers and platforms expose worker logs in different ways. Specific documentation for your environment will point out how to retrieve these logs.

Requirements
------------

The **R Runtime** is expected to be pre-installed in the cluster for `spark_apply` to function. Failure to install the cluster will trigger a `Cannot run program, no such file or directory` error while attempting to use `spark_apply()`. Contact your cluster administrator to consider making the R runtime available throughout the entire cluster.

A **Homogeneous Cluster** is required since the driver node distributes, and potentially compiles, packages to the workers. For instance, the driver and workers must have the same processor architecture, system libraries, etc.

Configuration
-------------

The following table describes relevant parameters while making use of `spark_apply`.

<table>
<colgroup>
<col width="38%" />
<col width="61%" />
</colgroup>
<thead>
<tr class="header">
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><code>spark.r.command</code></td>
<td>The path to the R binary. Useful to select from multiple R versions.</td>
</tr>
<tr class="even">
<td><code>sparklyr.worker.gateway.address</code></td>
<td>The gateway address to use under each worker node. Defaults to <code>sparklyr.gateway.address</code>.</td>
</tr>
<tr class="odd">
<td><code>sparklyr.worker.gateway.port</code></td>
<td>The gateway port to use under each worker node. Defaults to <code>sparklyr.gateway.port</code>.</td>
</tr>
</tbody>
</table>

For example, one could make use of an specific R version by running:

``` r
config <- spark_config()
config[["spark.r.command"]] <- "<path-to-r-version>"

sc <- spark_connect(master = "local", config = config)
sdf_len(sc, 10) %>% spark_apply(function(e) e)
```

Limitations
-----------

### Closures

Closures are serialized using `serialize`, which is described as "A simple low-level interface for serializing to connections.". One of the current limitations of `serialize` is that it wont serialize objects being referenced outside of it's environment. For instance, the following function will error out since the closures references `external_value`:

``` r
external_value <- 1
spark_apply(iris_tbl, function(e) e + external_value)
```

### Livy

Currently, Livy connections do not support distributing packages since the client machine where the libraries are precompiled might not have the same processor architecture, not operating systems that the cluster machines.

### Computing over Groups

While performing computations over groups, `spark_apply()` will provide partitions over the selected column; however, this implies that each partition can fit into a worker node, if this is not the case an exception will be thrown. To perform operations over groups that exceed the resources of a single node, one can consider partitioning to smaller units or use `dplyr::do` which is currently optimized for large partitions.

### Package Installation

Since packages are copied only once for the duration of the `spark_connect()` connection, installing additional packages is not supported while the connection is active. Therefore, if a new package needs to be installed, `spark_disconnect()` the connection, modify packages and reconnect.
