Spark Interface for R
================

[![Travis-CI Build Status](https://travis-ci.com/rstudio/spark.svg?token=MxiS2SHZy3QzqFf34wQr&branch=master)](https://travis-ci.com/rstudio/spark)

A set of tools to provision, connect and interface to Apache Spark from within the R language and ecosystem. This package supports connecting to local and remote Apache Spark clusters and provides support for R packages like dplyr and DBI.

Installation
------------

You can install the development version of the **spark** package using **devtools** as follows:

``` r
install.packages("devtools")
devtools::install_github("rstudio/spark")
```

You can then install various versions of Spark using the `spark_install` function:

``` r
library(spark)
spark_install(version = "nightly")
```

dplyr Interface
---------------

The spark package implements a dplyr back-end for Spark. Connect to Spark using the `spark_connect` function then obtain a dplyr interface using `src_spark` function:

``` r
# connect to local spark instance and get a dplyr interface
library(spark)
library(dplyr)
sc <- spark_connect("local[*]", version = "nightly")
db <- src_spark(sc)

# copy the flights table from the nycflights13 package to Spark
copy_to(db, nycflights13::flights, "flights")

# copy the Batting table from the Lahman package to Spark
copy_to(db, Lahman::Batting, "batting")
```

Then you can run dplyr against Spark:

``` r
# filter by departure delay and print the first few records
tbl(db, "flights") %>% filter(dep_delay == 2) %>% head
```

    ## Source: local data frame [6 x 16]
    ## 
    ##    year month   day dep_time dep_delay arr_time arr_delay carrier tailnum
    ## * <int> <int> <int>    <int>     <dbl>    <int>     <dbl>   <chr>   <chr>
    ## 1  2013     1     2      632         2      941         1      UA  N521UA
    ## 2  2013     1     2      647         2      903        15      US  N565UW
    ## 3  2013     1     2      734         2      844        -9      UA  N37408
    ## 4  2013     1     3     1219         2     1534       -22      UA  N36272
    ## 5  2013     1     4     1034         2     1238        -2      EV  N11137
    ## 6  2013     1     4     1116         2     1332       -13      UA  N35204
    ## Variables not shown: flight <int>, origin <chr>, dest <chr>, air_time
    ##   <dbl>, distance <dbl>, hour <dbl>, minute <dbl>.

[Introduction to dplyr](https://cran.rstudio.com/web/packages/dplyr/vignettes/introduction.html) provides additional dplyr examples you can try. For example, consider the last example from the tutorial which plots data on flight delays:

``` r
summarizeDelay <- function(source) {
  source %>% group_by(tailnum) %>%
    summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
    filter(count > 20, dist < 2000) %>%
    collect
}

delay <- tbl(db, "flights") %>% summarizeDelay

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area()
```

![](README_files/figure-markdown_github/unnamed-chunk-4-1.png)

### Window Functions

dplyr [window functions](https://cran.r-project.org/web/packages/dplyr/vignettes/window-functions.html) are also supported, for example:

``` r
topPlayers <- function(source) {
  source %>%
    select(playerID, yearID, teamID, G, AB:H) %>%
    arrange(playerID, yearID, teamID) %>%
    group_by(playerID) %>%
    filter(min_rank(desc(H)) <= 2 & H > 0) %>%
    collect
}

tbl(db, "batting") %>% topPlayers
```

    ## Source: local data frame [100,000 x 7]
    ## Groups: playerID [14,164]
    ## 
    ##     playerID yearID teamID     G    AB     R     H
    ## *      <chr>  <int>  <chr> <int> <int> <int> <int>
    ## 1  anderal01   1941    PIT    70   223    32    48
    ## 2  anderal01   1942    PIT    54   166    24    45
    ## 3  balesco01   2008    WAS    15    15     1     3
    ## 4  balesco01   2009    WAS     7     8     0     1
    ## 5  bandoch01   1986    CLE    92   254    28    68
    ## 6  bandoch01   1984    CLE    75   220    38    64
    ## 7  bedelho01   1962    ML1    58   138    15    27
    ## 8  bedelho01   1968    PHI     9     7     0     1
    ## 9  biittla01   1977    CHN   138   493    74   147
    ## 10 biittla01   1975    MON   121   346    34   109
    ## ..       ...    ...    ...   ...   ...   ...   ...

EC2
---

To start a new 1-master 1-slave Spark cluster in EC2 run the following code:

``` r
library(spark)
master <- start_ec2(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
                    secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                    pem_file = "sparkster.pem")
          
sc <- spark_connect(master)
```

The `access_key_id`, `secret_access_key` and `pem_file` need to be retrieved from the AWS console.

Extensibility
-------------

Spark provides low level access to native JVM objects, this topic targets users creating packages based on low-level spark integration. Here's an example of an R `count_lines` function built by calling Spark functions for reading and counting the lines of a text file.

``` r
# define an R interface to Spark line counting
count_lines <- function(scon, path) {
  spark_context(scon) %>%
    spark_invoke("textFile", path) %>%
    spark_invoke("count")
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

Performance
-----------

``` r
system.time(nycflights13::flights %>% summarizeDelay)
```

    ##    user  system elapsed 
    ##   0.091   0.001   0.096

``` r
system.time(tbl(db, "flights") %>%  summarizeDelay)
```

    ##    user  system elapsed 
    ##   0.397   0.012   1.069

``` r
system.time(Lahman::Batting %>% topPlayers)
```

    ##    user  system elapsed 
    ##   0.788   0.013   0.803

``` r
system.time(tbl(db, "batting") %>% topPlayers)
```

    ##    user  system elapsed 
    ##   4.195   0.032   9.602

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

    ## [Stage 43:==========================>                            (96 + 8) / 200]
    ## [Stage 43:============================>                         (107 + 8) / 200]
    ## [Stage 43:================================>                     (120 + 8) / 200]
    ## [Stage 43:===================================>                  (130 + 8) / 200]
    ## [Stage 43:======================================>               (142 + 8) / 200]
    ## [Stage 43:=========================================>            (154 + 8) / 200]
    ## [Stage 43:============================================>         (165 + 8) / 200]
    ## [Stage 43:===============================================>      (175 + 8) / 200]
    ## [Stage 43:===================================================>  (190 + 8) / 200]
    ## 

Finally, we disconnect from Spark:

``` r
spark_disconnect(sc)
```
