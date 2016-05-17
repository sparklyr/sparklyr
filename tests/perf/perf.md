Performance Benchmark
================

Initialization
--------------

``` r
knitr::opts_chunk$set(warning = FALSE, cache = FALSE)
library(spark)
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
library(reshape2)
library(ggplot2)
```

    ## Warning: package 'ggplot2' was built under R version 3.2.4

``` r
summarize_delay <- function(source) {
  source %>%
    group_by(tailnum) %>%
    summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
    filter(count > 20, dist < 2000)
}

top_players <- function(source) {
  source %>%
    select(playerID, yearID, teamID, G, AB:H) %>%
    arrange(playerID, yearID, teamID) %>%
    group_by(playerID) %>%
    filter(min_rank(desc(H)) <= 2 & H > 0)
}
```

``` r
spark_perf_test <- function(params, tests) {
  resultsList <- lapply(params, function(param) {
    spark_install(version = param$version, reset = TRUE, logging = param$logging)
    
    sc <- spark_connect(master = param$master,
                        version = param$version,
                        cores = param$cores)
    
    db <- src_spark(sc)
    
    copy_to(db,
            nycflights13::flights,
            "flights",
            cache = param$cache,
            repartition = param$partitions)
    
    copy_to(db,
            Lahman::Batting,
            "batting",
            cache = param$cache,
            repartition = param$partitions)
    
    sources <- list(
      flights = tbl(db, "flights"),
      batting = tbl(db, "batting")
    )
    
    testResults <- lapply(seq_along(tests), function(testNames, testNum) {
      test <-  tests[[testNum]]
      testName <- names(tests)[[testNum]]
      
      unname(c(
        lapply(param, function(e) if (is.null(e)) NA else e),
        list(
          test = testName,
          elapsed = system.time(test(db, sources))[["elapsed"]]
        )
      ))
    }, testNames = names(tests))
    
    spark_disconnect(sc)
    
    testResults
  })
  
  columnNames <- c(names(params[[1]]), list("test", "elapsed"))
  
  resultsDF <- do.call(rbind.data.frame, unlist(resultsList, recursive = FALSE))
  #resultsDF <- data.frame(do.call(rbind, unlist(resultsList, recursive = FALSE)))
  
  colnames(resultsDF) <- columnNames
  
  resultsDF
}
```

``` r
spark_perf_single_test <- function(runResults, master, version, logging, cache, partitions, cores) {
  run <- length(runResults)
  
  c(
    runResults,
    list(
      spark_perf_test(
        params = list(
          list(
            run = run,
            master = master,
            version = version,
            logging = logging,
            cache = cache,
            partitions = partitions,
            cores = cores
          )
        ),
        tests = list(
          `spark summarize` = function(db, sources) {
            sources$flights %>% summarize_delay %>% head
          },
          `dplyr summarize` = function(db, sources) {
            nycflights13::flights %>% summarize_delay %>% head
          },
          `spark rank` = function(db, sources) {
            sources$batting %>% top_players %>% head
          },
          `dplyr rank` = function(db, sources) {
            Lahman::Batting %>% top_players %>% head
          }
        )
      )
    )
  )
}
```

Results
-------

``` r
runResults <- list()

runResults <- spark_perf_single_test(runResults, "local", "1.6.0", "INFO", FALSE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local", "1.6.0", "INFO", TRUE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local", "2.0.0", "INFO", FALSE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local", "2.0.0", "INFO", TRUE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "1.6.0", "INFO", FALSE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "1.6.0", "WARN", FALSE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "1.6.0", "WARN", TRUE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "1.6.0", "WARN", TRUE, 8, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "2.0.0", "WARN", TRUE, 8, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "2.0.0", "WARN", TRUE, 0, 0)
runResults <- spark_perf_single_test(runResults, "local[*]", "1.6.0", "WARN", TRUE, 0, NULL)
runResults <- spark_perf_single_test(runResults, "local[*]", "2.0.0", "WARN", TRUE, 0, NULL)

results <- do.call("rbind", runResults)

results <- results %>% 
  mutate(params = paste(run, master, version, cache, logging, partitions))
```

``` r
results %>%
  filter(test == "spark summarize" | test == "dplyr summarize") %>%
  dcast(run + master + version + logging + partitions + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging partitions cores dplyr summarize
    ## 1    0    local   1.6.0    INFO          0     0           0.089
    ## 2    1    local   1.6.0    INFO          0     0           0.091
    ## 3    2    local   2.0.0    INFO          0     0           0.092
    ## 4    3    local   2.0.0    INFO          0     0           0.088
    ## 5    4 local[*]   1.6.0    INFO          0     0           0.086
    ## 6    5 local[*]   1.6.0    WARN          0     0           0.088
    ## 7    6 local[*]   1.6.0    WARN          0     0           0.088
    ## 8    7 local[*]   1.6.0    WARN          8     0           0.085
    ## 9    8 local[*]   2.0.0    WARN          8     0           0.089
    ## 10   9 local[*]   2.0.0    WARN          0     0           0.089
    ## 11  10 local[*]   1.6.0    WARN          0    NA           0.091
    ## 12  11 local[*]   2.0.0    WARN          0    NA           0.088
    ##    spark summarize
    ## 1            2.978
    ## 2            0.524
    ## 3            2.004
    ## 4            0.680
    ## 5            2.235
    ## 6            2.193
    ## 7            0.564
    ## 8            0.747
    ## 9            0.820
    ## 10           0.839
    ## 11           0.527
    ## 12           0.636

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  dcast(run + master + version + logging + partitions + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging partitions cores dplyr rank spark rank
    ## 1    0    local   1.6.0    INFO          0     0      0.815     13.268
    ## 2    1    local   1.6.0    INFO          0     0      0.829     12.346
    ## 3    2    local   2.0.0    INFO          0     0      0.763      6.412
    ## 4    3    local   2.0.0    INFO          0     0      0.759      6.095
    ## 5    4 local[*]   1.6.0    INFO          0     0      0.889      6.260
    ## 6    5 local[*]   1.6.0    WARN          0     0      0.890      6.216
    ## 7    6 local[*]   1.6.0    WARN          0     0      0.941      6.099
    ## 8    7 local[*]   1.6.0    WARN          8     0      0.916      6.147
    ## 9    8 local[*]   2.0.0    WARN          8     0      0.828      3.004
    ## 10   9 local[*]   2.0.0    WARN          0     0      0.847      2.866
    ## 11  10 local[*]   1.6.0    WARN          0    NA      0.790      1.365
    ## 12  11 local[*]   2.0.0    WARN          0    NA      0.796      0.962

``` r
results %>%
  filter(test == "spark rank" | test == "spark summarize") %>%
  ggplot(aes(test, params)) + 
    geom_tile(aes(fill = elapsed), colour = "white") +
    scale_fill_gradient(low = "steelblue", high = "black")
```

![](perf_files/figure-markdown_github/unnamed-chunk-8-1.png)

``` r
results %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_files/figure-markdown_github/unnamed-chunk-9-1.png)
