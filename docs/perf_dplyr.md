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
    
    shuffle <- getOption("rspark.dplyr.optimize_shuffle_cores", NULL)
    options(rspark.dplyr.optimize_shuffle_cores = param$shuffle)
    on.exit(options(rspark.dplyr.optimize_shuffle_cores = shuffle))
    
    sc <- spark_connect(master = param$master, cores = param$cores, version = param$version, codegen = param$codegen)
    
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
spark_perf_single_test <- function(
  runResults,
  master,
  cores,
  version,
  logging,
  cache,
  partitions,
  optimizeShuffleForCores,
  codegen) {
  
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
            shuffle = optimizeShuffleForCores,
            codegen = codegen
          )
        ),
        tests = list(
          `spark` = function(db, sources) {
            sources$flights %>% summarize_delay %>% head
          },
          `dplyr` = function(db, sources) {
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

runResults <- spark_perf_single_test(runResults, "local", NULL, "1.6.0", "INFO", FALSE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "1.6.0", "INFO", TRUE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "2.0.0-preview", "INFO", FALSE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", NULL, "2.0.0-preview", "INFO", TRUE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "INFO", FALSE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", FALSE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 8, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0-preview", "WARN", TRUE, 8, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0-preview", "WARN", TRUE, 0, FALSE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "1.6.0", "WARN", TRUE, 0, TRUE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0-preview", "WARN", TRUE, 0, TRUE, FALSE)
runResults <- spark_perf_single_test(runResults, "local", "auto", "2.0.0-preview", "WARN", TRUE, 0, TRUE, TRUE)

results <- do.call("rbind", runResults)

results <- results %>% 
  mutate(params = paste(run, version, cores, cache, logging, partitions, shuffle, codegen))
```

``` r
results %>%
  filter(test == "spark" | test == "dplyr") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle + codegen ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle codegen dplyr spark
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE   FALSE 0.101 3.144
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE   FALSE 0.090 0.515
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE   FALSE 0.084 1.986
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE   FALSE 0.085 0.582
    ## 5    4  auto         1.6.0    INFO    0   FALSE   FALSE 0.086 2.223
    ## 6    5  auto         1.6.0    WARN    0   FALSE   FALSE 0.087 2.181
    ## 7    6  auto         1.6.0    WARN    0   FALSE   FALSE 0.092 0.584
    ## 8    7  auto         1.6.0    WARN    8   FALSE   FALSE 0.088 0.738
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE   FALSE 0.087 0.949
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE   FALSE 0.105 0.673
    ## 11  10  auto         1.6.0    WARN    0    TRUE   FALSE 0.094 0.485
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE   FALSE 0.094 0.554
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE    TRUE 0.086 0.687

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle dplyr rank spark rank
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE      0.793     12.430
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE      0.753     11.473
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE      0.766      5.886
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE      0.753      5.587
    ## 5    4  auto         1.6.0    INFO    0   FALSE      0.876      6.169
    ## 6    5  auto         1.6.0    WARN    0   FALSE      0.870      6.107
    ## 7    6  auto         1.6.0    WARN    0   FALSE      0.873      5.409
    ## 8    7  auto         1.6.0    WARN    8   FALSE      0.878      5.742
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE      0.805      2.724
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE      0.841      2.656
    ## 11  10  auto         1.6.0    WARN    0    TRUE      0.813      1.436
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE      0.791      0.733
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE      0.801      0.787

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle dplyr warm spark warm
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE      0.773     10.938
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE      0.739      9.889
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE      0.735      5.053
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE      0.741      4.789
    ## 5    4  auto         1.6.0    INFO    0   FALSE      0.864      4.729
    ## 6    5  auto         1.6.0    WARN    0   FALSE      0.856      4.999
    ## 7    6  auto         1.6.0    WARN    0   FALSE      0.856      3.965
    ## 8    7  auto         1.6.0    WARN    8   FALSE      0.898      4.181
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE      0.813      2.175
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE      0.817      2.082
    ## 11  10  auto         1.6.0    WARN    0    TRUE      0.807      0.517
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE      0.782      0.403
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE      0.758      0.477

``` r
results %>%
  filter(test != "dplyr" | test != "spark") %>%
  ggplot(aes(test, params)) + 
    geom_tile(aes(fill = elapsed), colour = "white") +
    scale_fill_gradient(low = "steelblue", high = "black") +
    theme(axis.text.x=element_text(angle=330, hjust = 0))
```

![](perf_dplyr_files/figure-markdown_github/unnamed-chunk-9-1.png)

``` r
results %>%
  filter(test == "dplyr" | test == "spark") %>%
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
