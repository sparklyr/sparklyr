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
    spark_install(sparkVersion = param$version, reset = TRUE, logging = param$logging)
    
    shuffle <- getOption("rspark.dplyr.optimizeShuffleForCores", NULL)
    options(rspark.dplyr.optimizeShuffleForCores = param$shuffle)
    on.exit(options(rspark.dplyr.optimizeShuffleForCores = shuffle))
    
    sc <- spark_connect(master = param$master, cores = param$cores, version = param$version)
    
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
  
  colnames(resultsDF) <- columnNames
  
  resultsDF
}
```

``` r
spark_perf_single_test <- function(runResults, master, cores, version, logging, cache, partitions, optimizeShuffleForCores) {
  run <- length(runResults)
  
  c(
    runResults,
    list(
      spark_perf_test(
        params = list(
          list(
            run = run,
            master = master,
            cores = cores,
            version = version,
            logging = logging,
            cache = cache,
            partitions = partitions,
            shuffle = optimizeShuffleForCores
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

runResults <- spark_perf_single_test(runResults, "local", NULL, "1.6.0", "INFO", FALSE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "1.6.0", "INFO", TRUE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "2.0.0", "INFO", FALSE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "2.0.0", "INFO", TRUE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "INFO", FALSE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", FALSE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 8, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0", "WARN", TRUE, 8, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0", "WARN", TRUE, 0, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 0, TRUE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0", "WARN", TRUE, 0, TRUE)

results <- do.call("rbind", runResults)

results <- results %>% 
  mutate(params = paste(run, version, cores, cache, logging, partitions, shuffle))
```

``` r
results %>%
  filter(test == "spark summarize" | test == "dplyr summarize") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores version logging part shuffle dplyr summarize spark summarize
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE           0.086           3.005
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE           0.092           0.577
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE           0.093           1.975
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE           0.083           0.672
    ## 5    4  auto   1.6.0    INFO    0   FALSE           0.085           2.272
    ## 6    5  auto   1.6.0    WARN    0   FALSE           0.084           2.187
    ## 7    6  auto   1.6.0    WARN    0   FALSE           0.086           0.658
    ## 8    7  auto   1.6.0    WARN    8   FALSE           0.089           0.767
    ## 9    8  auto   2.0.0    WARN    8   FALSE           0.098           0.930
    ## 10   9  auto   2.0.0    WARN    0   FALSE           0.102           0.818
    ## 11  10  auto   1.6.0    WARN    0    TRUE           0.094           0.480
    ## 12  11  auto   2.0.0    WARN    0    TRUE           0.084           0.670

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores version logging part shuffle dplyr rank spark rank
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE      0.815     13.050
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE      0.898     12.125
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE      0.810      6.616
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE      0.845      6.278
    ## 5    4  auto   1.6.0    INFO    0   FALSE      0.914      6.262
    ## 6    5  auto   1.6.0    WARN    0   FALSE      0.886      7.978
    ## 7    6  auto   1.6.0    WARN    0   FALSE      0.957      5.746
    ## 8    7  auto   1.6.0    WARN    8   FALSE      0.898      6.246
    ## 9    8  auto   2.0.0    WARN    8   FALSE      0.811      3.070
    ## 10   9  auto   2.0.0    WARN    0   FALSE      0.858      2.925
    ## 11  10  auto   1.6.0    WARN    0    TRUE      0.807      1.572
    ## 12  11  auto   2.0.0    WARN    0    TRUE      0.850      0.925

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores version logging part shuffle dplyr warm spark warm
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE      0.748     11.669
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE      0.850     10.512
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE      0.785      5.321
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE      0.771      5.119
    ## 5    4  auto   1.6.0    INFO    0   FALSE      0.914      4.856
    ## 6    5  auto   1.6.0    WARN    0   FALSE      0.977      5.509
    ## 7    6  auto   1.6.0    WARN    0   FALSE      0.883      4.042
    ## 8    7  auto   1.6.0    WARN    8   FALSE      0.906      4.418
    ## 9    8  auto   2.0.0    WARN    8   FALSE      0.870      2.356
    ## 10   9  auto   2.0.0    WARN    0   FALSE      0.854      2.245
    ## 11  10  auto   1.6.0    WARN    0    TRUE      0.774      0.604
    ## 12  11  auto   2.0.0    WARN    0    TRUE      0.790      0.573

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
