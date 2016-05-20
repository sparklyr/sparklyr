Performance Benchmark
================

Initialization
--------------

``` r
knitr::opts_chunk$set(warning = FALSE, cache = FALSE)
library(rspark)
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

top_players_by_run <- function(source) {
  source %>%
    select(playerID, yearID, teamID, G, AB:H) %>%
    arrange(playerID, yearID, teamID) %>%
    group_by(playerID) %>%
    filter(min_rank(desc(R)) <= 2 & R > 0)
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
          },
          `spark warm` = function(db, sources) {
            sources$batting %>% top_players_by_run %>% head
          },
          `dplyr warm` = function(db, sources) {
            Lahman::Batting %>% top_players_by_run %>% head
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
  rename(part = partitions) %>%
  dcast(run + master + version + logging + part + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging part cores dplyr summarize spark summarize
    ## 1    0    local   1.6.0    INFO    0     0           0.090           3.069
    ## 2    1    local   1.6.0    INFO    0     0           0.091           0.514
    ## 3    2    local   2.0.0    INFO    0     0           0.096           2.323
    ## 4    3    local   2.0.0    INFO    0     0           0.089           0.704
    ## 5    4 local[*]   1.6.0    INFO    0     0           0.091           2.283
    ## 6    5 local[*]   1.6.0    WARN    0     0           0.089           2.213
    ## 7    6 local[*]   1.6.0    WARN    0     0           0.094           0.600
    ## 8    7 local[*]   1.6.0    WARN    8     0           0.093           0.727
    ## 9    8 local[*]   2.0.0    WARN    8     0           0.090           0.885
    ## 10   9 local[*]   2.0.0    WARN    0     0           0.088           0.793
    ## 11  10 local[*]   1.6.0    WARN    0    NA           0.089           0.459
    ## 12  11 local[*]   2.0.0    WARN    0    NA           0.096           0.661

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + master + version + logging + part + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging part cores dplyr rank spark rank
    ## 1    0    local   1.6.0    INFO    0     0      0.790     13.315
    ## 2    1    local   1.6.0    INFO    0     0      0.866     12.760
    ## 3    2    local   2.0.0    INFO    0     0      0.796      6.563
    ## 4    3    local   2.0.0    INFO    0     0      0.797      6.089
    ## 5    4 local[*]   1.6.0    INFO    0     0      0.950      6.659
    ## 6    5 local[*]   1.6.0    WARN    0     0      0.932      6.770
    ## 7    6 local[*]   1.6.0    WARN    0     0      0.885      5.675
    ## 8    7 local[*]   1.6.0    WARN    8     0      0.941      6.155
    ## 9    8 local[*]   2.0.0    WARN    8     0      0.839      2.830
    ## 10   9 local[*]   2.0.0    WARN    0     0      0.894      2.759
    ## 11  10 local[*]   1.6.0    WARN    0    NA      0.828      1.503
    ## 12  11 local[*]   2.0.0    WARN    0    NA      0.803      0.799

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + master + version + logging + part + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging part cores dplyr warm spark warm
    ## 1    0    local   1.6.0    INFO    0     0      0.810     11.753
    ## 2    1    local   1.6.0    INFO    0     0      0.775     11.155
    ## 3    2    local   2.0.0    INFO    0     0      0.763      5.573
    ## 4    3    local   2.0.0    INFO    0     0      0.835      5.403
    ## 5    4 local[*]   1.6.0    INFO    0     0      0.930      4.993
    ## 6    5 local[*]   1.6.0    WARN    0     0      0.940      5.006
    ## 7    6 local[*]   1.6.0    WARN    0     0      0.876      4.094
    ## 8    7 local[*]   1.6.0    WARN    8     0      0.914      4.719
    ## 9    8 local[*]   2.0.0    WARN    8     0      0.856      2.721
    ## 10   9 local[*]   2.0.0    WARN    0     0      0.835      2.266
    ## 11  10 local[*]   1.6.0    WARN    0    NA      0.768      0.529
    ## 12  11 local[*]   2.0.0    WARN    0    NA      0.798      0.447

``` r
results %>%
  filter(test != "dplyr summarize" | test != "spark summarize") %>%
  ggplot(aes(test, params)) + 
    geom_tile(aes(fill = elapsed), colour = "white") +
    scale_fill_gradient(low = "steelblue", high = "black") +
    theme(axis.text.x=element_text(angle=330, hjust = 0))
```

![](perf_files/figure-markdown_github/unnamed-chunk-9-1.png)

``` r
results %>%
  filter(test == "dplyr summarize" | test == "spark summarize") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_files/figure-markdown_github/unnamed-chunk-10-1.png)

``` r
results %>%
  filter(test == "dplyr rank" | test == "spark rank") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_files/figure-markdown_github/unnamed-chunk-11-1.png)

``` r
results %>%
  filter(test == "dplyr warm" | test == "spark warm") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_files/figure-markdown_github/unnamed-chunk-12-1.png)
