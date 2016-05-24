RSpark Performance: Dplyr Queries
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
    ## 1    0    local   1.6.0    INFO    0     0           0.093           2.992
    ## 2    1    local   1.6.0    INFO    0     0           0.091           0.528
    ## 3    2    local   2.0.0    INFO    0     0           0.094           1.989
    ## 4    3    local   2.0.0    INFO    0     0           0.088           0.700
    ## 5    4 local[*]   1.6.0    INFO    0     0           0.087           2.246
    ## 6    5 local[*]   1.6.0    WARN    0     0           0.088           2.219
    ## 7    6 local[*]   1.6.0    WARN    0     0           0.092           0.539
    ## 8    7 local[*]   1.6.0    WARN    8     0           0.090           0.780
    ## 9    8 local[*]   2.0.0    WARN    8     0           0.085           0.988
    ## 10   9 local[*]   2.0.0    WARN    0     0           0.089           0.782
    ## 11  10 local[*]   1.6.0    WARN    0    NA           0.088           0.470
    ## 12  11 local[*]   2.0.0    WARN    0    NA           0.089           0.688

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + master + version + logging + part + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging part cores dplyr rank spark rank
    ## 1    0    local   1.6.0    INFO    0     0      0.805     12.863
    ## 2    1    local   1.6.0    INFO    0     0      0.841     11.749
    ## 3    2    local   2.0.0    INFO    0     0      0.786      6.096
    ## 4    3    local   2.0.0    INFO    0     0      0.773      5.913
    ## 5    4 local[*]   1.6.0    INFO    0     0      0.896      6.171
    ## 6    5 local[*]   1.6.0    WARN    0     0      0.900      6.212
    ## 7    6 local[*]   1.6.0    WARN    0     0      0.968      5.871
    ## 8    7 local[*]   1.6.0    WARN    8     0      0.960      6.170
    ## 9    8 local[*]   2.0.0    WARN    8     0      0.832      2.819
    ## 10   9 local[*]   2.0.0    WARN    0     0      0.843      2.820
    ## 11  10 local[*]   1.6.0    WARN    0    NA      0.794      1.493
    ## 12  11 local[*]   2.0.0    WARN    0    NA      0.798      0.886

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + master + version + logging + part + cores ~ test, value.var = "elapsed")
```

    ##    run   master version logging part cores dplyr warm spark warm
    ## 1    0    local   1.6.0    INFO    0     0      0.774     11.417
    ## 2    1    local   1.6.0    INFO    0     0      0.776     10.507
    ## 3    2    local   2.0.0    INFO    0     0      0.769      5.262
    ## 4    3    local   2.0.0    INFO    0     0      0.767      4.997
    ## 5    4 local[*]   1.6.0    INFO    0     0      0.882      4.777
    ## 6    5 local[*]   1.6.0    WARN    0     0      0.886      4.734
    ## 7    6 local[*]   1.6.0    WARN    0     0      0.943      4.336
    ## 8    7 local[*]   1.6.0    WARN    8     0      0.937      4.644
    ## 9    8 local[*]   2.0.0    WARN    8     0      0.823      2.268
    ## 10   9 local[*]   2.0.0    WARN    0     0      0.826      2.258
    ## 11  10 local[*]   1.6.0    WARN    0    NA      0.788      0.582
    ## 12  11 local[*]   2.0.0    WARN    0    NA      0.783      0.447

``` r
results %>%
  filter(test != "dplyr summarize" | test != "spark summarize") %>%
  ggplot(aes(test, params)) + 
    geom_tile(aes(fill = elapsed), colour = "white") +
    scale_fill_gradient(low = "steelblue", high = "black") +
    theme(axis.text.x=element_text(angle=330, hjust = 0))
```

![](perf_dplyr_files/figure-markdown_github/unnamed-chunk-9-1.png)

``` r
results %>%
  filter(test == "dplyr summarize" | test == "spark summarize") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_dplyr_files/figure-markdown_github/unnamed-chunk-10-1.png)

``` r
results %>%
  filter(test == "dplyr rank" | test == "spark rank") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_dplyr_files/figure-markdown_github/unnamed-chunk-11-1.png)

``` r
results %>%
  filter(test == "dplyr warm" | test == "spark warm") %>%
  ggplot(aes(x=run, y=elapsed, group = test, color = test)) + 
    geom_line() + geom_point() +
    ggtitle("Time per Run")
```

![](perf_dplyr_files/figure-markdown_github/unnamed-chunk-12-1.png)
