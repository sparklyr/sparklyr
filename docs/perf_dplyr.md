Performance: Dplyr Queries
================

Initialization
--------------

``` r
knitr::opts_chunk$set(warning = FALSE, cache = FALSE)
library(sparklyr)
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
    
    config <- spark_config()
    config[["spark.sql.shuffle.partitions"]] <- if(param$shuffle) parallel::detectCores() else NULL
    config[["spark.sql.codegen.wholeStage"]] <- param$codegen
    
    if (is.null(param$cores)) {
      config[["sparklyr.cores.local"]] <- param$cores
    }
    
    sc <- spark_connect(master = param$master, version = param$version, config = config)
    
    copy_to(sc,
            nycflights13::flights,
            "flights",
            memory = param$cache,
            repartition = param$partitions)
    
    copy_to(sc,
            Lahman::Batting,
            "batting",
            memory = param$cache,
            repartition = param$partitions)
    
    sources <- list(
      flights = tbl(sc, "flights"),
      batting = tbl(sc, "batting")
    )
    
    testResults <- lapply(seq_along(tests), function(testNames, testNum) {
      test <-  tests[[testNum]]
      testName <- names(tests)[[testNum]]
      
      unname(c(
        lapply(param, function(e) if (is.null(e)) NA else e),
        list(
          test = testName,
          elapsed = system.time(test(db, sources) %>% collect)[["elapsed"]]
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
            shuffle = optimizeShuffleForCores,
            codegen = codegen,
            partitions = partitions
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
  mutate(params = paste(run, version, cores, cache, logging, partitions, shuffle))
```

``` r
results %>%
  filter(test == "spark" | test == "dplyr") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle + codegen ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle codegen dplyr spark
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE   FALSE 0.092 2.915
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE   FALSE 0.092 0.645
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE   FALSE 0.092 1.493
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE   FALSE 0.088 0.654
    ## 5    4  auto         1.6.0    INFO    0   FALSE   FALSE 0.086 1.957
    ## 6    5  auto         1.6.0    WARN    0   FALSE   FALSE 0.088 2.020
    ## 7    6  auto         1.6.0    WARN    0   FALSE   FALSE 0.090 0.563
    ## 8    7  auto         1.6.0    WARN    8   FALSE   FALSE 0.088 0.663
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE   FALSE 0.087 1.004
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE   FALSE 0.096 1.110
    ## 11  10  auto         1.6.0    WARN    0    TRUE   FALSE 0.089 0.520
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE   FALSE 0.088 0.650
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE    TRUE 0.088 0.653

``` r
results %>%
  filter(test == "spark rank" | test == "dplyr rank") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle dplyr rank spark rank
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE      0.797     13.495
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE      0.778     12.450
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE      0.866      6.387
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE      0.792      6.053
    ## 5    4  auto         1.6.0    INFO    0   FALSE      0.881      6.224
    ## 6    5  auto         1.6.0    WARN    0   FALSE      0.927      6.483
    ## 7    6  auto         1.6.0    WARN    0   FALSE      0.889      5.806
    ## 8    7  auto         1.6.0    WARN    8   FALSE      0.910      6.826
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE      0.828      2.927
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE      0.840      2.734
    ## 11  10  auto         1.6.0    WARN    0    TRUE      0.776      1.465
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE      0.776      0.909
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE      0.794      0.759

``` r
results %>%
  filter(test == "spark warm" | test == "dplyr warm") %>%
  rename(part = partitions) %>%
  dcast(run + cores + version + logging + part + shuffle ~ test, value.var = "elapsed")
```

    ##    run cores       version logging part shuffle dplyr warm spark warm
    ## 1    0  <NA>         1.6.0    INFO    0   FALSE      0.789     11.913
    ## 2    1  <NA>         1.6.0    INFO    0   FALSE      0.767     10.788
    ## 3    2  <NA> 2.0.0-preview    INFO    0   FALSE      0.774      5.474
    ## 4    3  <NA> 2.0.0-preview    INFO    0   FALSE      0.783      5.242
    ## 5    4  auto         1.6.0    INFO    0   FALSE      0.878      4.951
    ## 6    5  auto         1.6.0    WARN    0   FALSE      0.935      4.956
    ## 7    6  auto         1.6.0    WARN    0   FALSE      0.866      4.135
    ## 8    7  auto         1.6.0    WARN    8   FALSE      0.930      4.525
    ## 9    8  auto 2.0.0-preview    WARN    8   FALSE      0.815      2.275
    ## 10   9  auto 2.0.0-preview    WARN    0   FALSE      0.820      2.198
    ## 11  10  auto         1.6.0    WARN    0    TRUE      0.804      0.559
    ## 12  11  auto 2.0.0-preview    WARN    0    TRUE      0.765      0.386
    ## 13  12  auto 2.0.0-preview    WARN    0    TRUE      0.776      0.432

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
