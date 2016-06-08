Spark Interface for R
================

[![Travis-CI Build Status](https://travis-ci.com/rstudio/rspark.svg?token=MxiS2SHZy3QzqFf34wQr&branch=master)](https://travis-ci.com/rstudio/rspark)

A set of tools to provision, connect and interface to Apache Spark from within the R language and ecosystem. This package supports connecting to local and remote Apache Spark clusters and provides support for R packages like dplyr and DBI.

Installation
------------

You can install the development version of the **rspark** package using **devtools** as follows (note that installation of the development version of **devtools** itself is also required):

``` r
devtools::install_github("hadley/devtools")
devtools::reload(devtools::inst("devtools"))

devtools::install_github("rstudio/rspark", auth_token = "56aef3d82d3ef05755e40a4f6bdaab6fbed8a1f1")
```

You can then install various versions of Spark using the `spark_install` function:

``` r
library(rspark)
spark_install(version = "1.6.1", hadoop_version = "2.6", reset = TRUE)
```

dplyr Interface
---------------

The rspark package implements a dplyr back-end for Spark. Connect to Spark using the `spark_connect` function then obtain a dplyr interface using `src_spark` function:

``` r
# connect to local spark instance and get a dplyr interface
library(rspark)
library(dplyr)
sc <- spark_connect("local", cores = "auto", version = "1.6.1", memory = "8G")
db <- src_spark(sc)

# copy the flights table from the nycflights13 package to Spark
copy_to(db, nycflights13::flights, "flights")
flights <- tbl(db, "flights")

# copy the Batting table from the Lahman package to Spark
copy_to(db, Lahman::Batting, "batting")
batting <- tbl(db, "batting")
```

Then you can run dplyr against Spark:

``` r
# filter by departure delay and print the first few records
flights %>% filter(dep_delay == 2)
```

    ## Source:   query [?? x 16]
    ## Database: spark connection master=local app=rspark local=TRUE
    ## 
    ##     year month   day dep_time dep_delay arr_time arr_delay carrier tailnum
    ##    <int> <int> <int>    <int>     <dbl>    <int>     <dbl>   <chr>   <chr>
    ## 1   2013     1     1      517         2      830        11      UA  N14228
    ## 2   2013     1     1      542         2      923        33      AA  N619AA
    ## 3   2013     1     1      702         2     1058        44      B6  N779JB
    ## 4   2013     1     1      715         2      911        21      UA  N841UA
    ## 5   2013     1     1      752         2     1025        -4      UA  N511UA
    ## 6   2013     1     1      917         2     1206        -5      B6  N568JB
    ## 7   2013     1     1      932         2     1219        -6      VX  N641VA
    ## 8   2013     1     1     1028         2     1350        11      UA  N76508
    ## 9   2013     1     1     1042         2     1325        -1      B6  N529JB
    ## 10  2013     1     1     1231         2     1523        -6      UA  N402UA
    ## ..   ...   ...   ...      ...       ...      ...       ...     ...     ...
    ## Variables not shown: flight <int>, origin <chr>, dest <chr>, air_time
    ##   <dbl>, distance <dbl>, hour <dbl>, minute <dbl>.

[Introduction to dplyr](https://cran.rstudio.com/web/packages/dplyr/vignettes/introduction.html) provides additional dplyr examples you can try. For example, consider the last example from the tutorial which plots data on flight delays:

``` r
delay <- flights %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)
```

![](res/ggplot2-1.png)

### Window Functions

dplyr [window functions](https://cran.r-project.org/web/packages/dplyr/vignettes/window-functions.html) are also supported, for example:

``` r
batting %>%
  select(playerID, yearID, teamID, G, AB:H) %>%
  arrange(playerID, yearID, teamID) %>%
  group_by(playerID) %>%
  filter(min_rank(desc(H)) <= 2 & H > 0)
```

    ## Source:   query [?? x 7]
    ## Database: spark connection master=local app=rspark local=TRUE
    ## Groups: playerID
    ## 
    ##     playerID yearID teamID     G    AB     R     H
    ##        <chr>  <int>  <chr> <int> <int> <int> <int>
    ## 1  abbotpa01   2000    SEA    35     5     1     2
    ## 2  abbotpa01   2004    PHI    10    11     1     2
    ## 3  abnersh01   1992    CHA    97   208    21    58
    ## 4  abnersh01   1990    SDN    91   184    17    45
    ## 5  abreujo02   2014    CHA   145   556    80   176
    ## 6  acevejo01   2001    CIN    18    34     1     4
    ## 7  acevejo01   2004    CIN    39    43     0     2
    ## 8  adamsbe01   1919    PHI    78   232    14    54
    ## 9  adamsbe01   1918    PHI    84   227    10    40
    ## 10 adamsbu01   1945    SLN   140   578    98   169
    ## ..       ...    ...    ...   ...   ...   ...   ...

EC2
---

To start a new 1-master 1-slave Spark cluster in EC2 run the following code:

``` r
library(rspark)
ci <- spark_ec2_cluster(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
                        secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                        pem_file = "spark.pem")

spark_ec2_deploy(ci)

spark_ec2_web(ci)
spark_ec2_rstudio(ci)

spark_ec2_stop(ci)
spark_ec2_destroy(ci)
```

The `access_key_id`, `secret_access_key` and `pem_file` need to be retrieved from the AWS console.

For additional configuration and examples read: [Using RSpark in EC2](docs/ec2.md)

Extensibility
-------------

Spark provides low level access to native JVM objects, this topic targets users creating packages based on low-level spark integration. Here's an example of an R `count_lines` function built by calling Spark functions for reading and counting the lines of a text file.

``` r
# define an R interface to Spark line counting
count_lines <- function(sc, path) {
  file <- spark_invoke(sc, "textFile", path, as.integer(1))
  spark_invoke(file, "count")
}

# write a CSV 
tempfile <- tempfile(fileext = ".csv")
write.csv(nycflights13::flights, tempfile, row.names = FALSE, na = "")

# call spark to count the lines
count_lines(sc, tempfile)
```

    ## [1] 336777

Package authors can use this mechanism to create an R interface to any of Spark's underlying Java APIs.

dplyr Utilities
---------------

You can cache a table into memory with:

``` r
tbl_cache(db, "batting")
```

and unload from memory using:

``` r
tbl_uncache(db, "batting")
```

Connection Utilities
--------------------

You can view the Spark web console using the `spark_web` function:

``` r
spark_web(sc)
```

You can show the log using the `spark_log` function:

``` r
spark_log(sc, n = 10)
```

    ## 16/06/08 17:33:35 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 19 (/tmp/RtmpmoZRfs/file2b812ff78a82.csv MapPartitionsRDD[79] at textFile at NativeMethodAccessorImpl.java:-2)
    ## 16/06/08 17:33:35 INFO TaskSchedulerImpl: Adding task set 19.0 with 1 tasks
    ## 16/06/08 17:33:35 INFO TaskSetManager: Starting task 0.0 in stage 19.0 (TID 38, localhost, partition 0,PROCESS_LOCAL, 2441 bytes)
    ## 16/06/08 17:33:35 INFO Executor: Running task 0.0 in stage 19.0 (TID 38)
    ## 16/06/08 17:33:35 INFO HadoopRDD: Input split: file:/tmp/RtmpmoZRfs/file2b812ff78a82.csv:0+23367180
    ## 16/06/08 17:33:35 INFO Executor: Finished task 0.0 in stage 19.0 (TID 38). 2082 bytes result sent to driver
    ## 16/06/08 17:33:35 INFO TaskSetManager: Finished task 0.0 in stage 19.0 (TID 38) in 85 ms on localhost (1/1)
    ## 16/06/08 17:33:35 INFO DAGScheduler: ResultStage 19 (count at NativeMethodAccessorImpl.java:-2) finished in 0.085 s
    ## 16/06/08 17:33:35 INFO TaskSchedulerImpl: Removed TaskSet 19.0, whose tasks have all completed, from pool 
    ## 16/06/08 17:33:35 INFO DAGScheduler: Job 11 finished: count at NativeMethodAccessorImpl.java:-2, took 0.089154 s

Finally, we disconnect from Spark:

``` r
spark_disconnect(sc)
```

Additional Resources
--------------------

For performance runs under various parameters, read: [RSpark Performance](docs/perf_dplyr.md)
