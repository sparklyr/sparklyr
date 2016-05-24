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
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE           0.099           3.314
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE           0.098           0.532
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE           0.086           2.119
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE           0.104           0.669
    ## 5    4  auto   1.6.0    INFO    0   FALSE           0.091           2.174
    ## 6    5  auto   1.6.0    WARN    0   FALSE           0.089           2.230
    ## 7    6  auto   1.6.0    WARN    0   FALSE           0.089           0.574
    ## 8    7  auto   1.6.0    WARN    8   FALSE           0.085           0.745
    ## 9    8  auto   2.0.0    WARN    8   FALSE           0.092           0.806
    ## 10   9  auto   2.0.0    WARN    0   FALSE           0.101           0.796
    ## 11  10  auto   1.6.0    WARN    0    TRUE           0.091           0.523
    ## 12  11  auto   2.0.0    WARN    0    TRUE           0.091           0.643

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores version logging part shuffle dplyr rank spark rank
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE      0.903     14.289
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE      0.962     12.564
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE      0.789      6.304
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE      0.823      5.981
    ## 5    4  auto   1.6.0    INFO    0   FALSE      0.922      6.165
    ## 6    5  auto   1.6.0    WARN    0   FALSE      0.901      6.203
    ## 7    6  auto   1.6.0    WARN    0   FALSE      0.957      5.904
    ## 8    7  auto   1.6.0    WARN    8   FALSE      0.944      6.740
    ## 9    8  auto   2.0.0    WARN    8   FALSE      0.878      2.913
    ## 10   9  auto   2.0.0    WARN    0   FALSE      0.839      2.851
    ## 11  10  auto   1.6.0    WARN    0    TRUE      0.812      1.417
    ## 12  11  auto   2.0.0    WARN    0    TRUE      0.778      0.834

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores version logging part shuffle dplyr warm spark warm
    ## 1    0  <NA>   1.6.0    INFO    0   FALSE      0.865     11.572
    ## 2    1  <NA>   1.6.0    INFO    0   FALSE      0.783     10.689
    ## 3    2  <NA>   2.0.0    INFO    0   FALSE      0.804      5.442
    ## 4    3  <NA>   2.0.0    INFO    0   FALSE      0.756      5.224
    ## 5    4  auto   1.6.0    INFO    0   FALSE      0.889      4.687
    ## 6    5  auto   1.6.0    WARN    0   FALSE      0.932      4.921
    ## 7    6  auto   1.6.0    WARN    0   FALSE      0.961      4.257
    ## 8    7  auto   1.6.0    WARN    8   FALSE      0.916      4.698
    ## 9    8  auto   2.0.0    WARN    8   FALSE      0.826      2.416
    ## 10   9  auto   2.0.0    WARN    0   FALSE      0.801      2.383
    ## 11  10  auto   1.6.0    WARN    0    TRUE      0.783      0.579
    ## 12  11  auto   2.0.0    WARN    0    TRUE      0.799      0.458

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
