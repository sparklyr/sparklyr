Manipulating Data with dplyr
================

## Overview

[**dplyr**](https://cran.r-project.org/web/packages/dplyr/index.html) is
an R package for working with structured data both in and outside of R.
dplyr makes data manipulation for R users easy, consistent, and
performant. With dplyr as an interface to manipulating Spark DataFrames,
you can:

  - Select, filter, and aggregate data
  - Use window functions (e.g. for sampling)
  - Perform joins on DataFrames
  - Collect data from Spark into R

Statements in dplyr can be chained together using pipes defined by the
[magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)
R package. dplyr also supports [non-standard
evalution](https://cran.r-project.org/web/packages/dplyr/vignettes/nse.html)
of its arguments. For more information on dplyr, see the
[introduction](https://cran.r-project.org/web/packages/dplyr/vignettes/introduction.html),
a guide for connecting to
[databases](https://cran.r-project.org/web/packages/dplyr/vignettes/databases.html),
and a variety of
[vignettes](https://cran.r-project.org/web/packages/dplyr/index.html).

## Reading Data

You can read data into Spark DataFrames using the following
functions:

| Function                                               | Description                                                           |
| ------------------------------------------------------ | --------------------------------------------------------------------- |
| [`spark_read_csv`](/reference/spark_read_csv/)         | Reads a CSV file and provides a data source compatible with dplyr     |
| [`spark_read_json`](/reference/spark_read_json/)       | Reads a JSON file and provides a data source compatible with dplyr    |
| [`spark_read_parquet`](/reference/spark_read_parquet/) | Reads a parquet file and provides a data source compatible with dplyr |

Regardless of the format of your data, Spark supports reading data from
a variety of different data sources. These include data stored on HDFS
(`hdfs://` protocol), Amazon S3 (`s3n://` protocol), or local files
available to the Spark worker nodes (`file://` protocol)

Each of these functions returns a reference to a Spark DataFrame which
can be used as a dplyr table (`tbl`).

### Flights Data

This guide will demonstrate some of the basic data manipulation verbs of
dplyr by using data from the `nycflights13` R package. This package
contains data for all 336,776 flights departing New York City in 2013.
It also includes useful metadata on airlines, airports, weather, and
planes. The data comes from the US [Bureau of Transportation
Statistics](http://www.transtats.bts.gov/DatabaseInfo.asp?DB_ID=120&Link=0),
and is documented in `?nycflights13`

Connect to the cluster and copy the flights data using the `copy_to`
function. Caveat: The flight data in `nycflights13` is convenient for
dplyr demonstrations because it is small, but in practice large data
should rarely be copied directly from R objects.

``` r
library(sparklyr)
library(dplyr)
library(nycflights13)
library(ggplot2)

sc <- spark_connect(master="local")
flights <- copy_to(sc, flights, "flights")
airlines <- copy_to(sc, airlines, "airlines")
src_tbls(sc)
```

    ## [1] "airlines" "flights"

## dplyr Verbs

Verbs are dplyr commands for manipulating data. When connected to a
Spark DataFrame, dplyr translates the commands into Spark SQL
statements. Remote data sources use exactly the same five verbs as local
data sources. Here are the five verbs with their corresponding SQL
commands:

  - `select` ~ `SELECT`
  - `filter` ~ `WHERE`
  - `arrange` ~ `ORDER`
  - `summarise` ~ `aggregators: sum, min, sd, etc.`
  - `mutate` ~ `operators: +, *, log, etc.`

<!-- end list -->

``` r
select(flights, year:day, arr_delay, dep_delay)
```

    ## # Source: lazy query [?? x 5]
    ## # Database: spark_connection
    ##     year month   day arr_delay dep_delay
    ##    <int> <int> <int>     <dbl>     <dbl>
    ##  1  2013     1     1     11.0       2.00
    ##  2  2013     1     1     20.0       4.00
    ##  3  2013     1     1     33.0       2.00
    ##  4  2013     1     1    -18.0      -1.00
    ##  5  2013     1     1    -25.0      -6.00
    ##  6  2013     1     1     12.0      -4.00
    ##  7  2013     1     1     19.0      -5.00
    ##  8  2013     1     1    -14.0      -3.00
    ##  9  2013     1     1    - 8.00     -3.00
    ## 10  2013     1     1      8.00     -2.00
    ## # ... with more rows

``` r
filter(flights, dep_delay > 1000)
```

    ## # Source: lazy query [?? x 19]
    ## # Database: spark_connection
    ##    year month   day dep_t~ sche~ dep_~ arr_~ sche~ arr_~ carr~ flig~ tail~
    ##   <int> <int> <int>  <int> <int> <dbl> <int> <int> <dbl> <chr> <int> <chr>
    ## 1  2013     1     9    641   900  1301  1242  1530  1272 HA       51 N384~
    ## 2  2013     1    10   1121  1635  1126  1239  1810  1109 MQ     3695 N517~
    ## 3  2013     6    15   1432  1935  1137  1607  2120  1127 MQ     3535 N504~
    ## 4  2013     7    22    845  1600  1005  1044  1815   989 MQ     3075 N665~
    ## 5  2013     9    20   1139  1845  1014  1457  2210  1007 AA      177 N338~
    ## # ... with 7 more variables: origin <chr>, dest <chr>, air_time <dbl>,
    ## #   distance <dbl>, hour <dbl>, minute <dbl>, time_hour <dbl>

``` r
arrange(flights, desc(dep_delay))
```

    ## # Source: table<flights> [?? x 19]
    ## # Database: spark_connection
    ## # Ordered by: desc(dep_delay)
    ##     year month   day dep_~ sche~ dep_~ arr_~ sche~ arr_~ carr~ flig~ tail~
    ##    <int> <int> <int> <int> <int> <dbl> <int> <int> <dbl> <chr> <int> <chr>
    ##  1  2013     1     9   641   900  1301  1242  1530  1272 HA       51 N384~
    ##  2  2013     6    15  1432  1935  1137  1607  2120  1127 MQ     3535 N504~
    ##  3  2013     1    10  1121  1635  1126  1239  1810  1109 MQ     3695 N517~
    ##  4  2013     9    20  1139  1845  1014  1457  2210  1007 AA      177 N338~
    ##  5  2013     7    22   845  1600  1005  1044  1815   989 MQ     3075 N665~
    ##  6  2013     4    10  1100  1900   960  1342  2211   931 DL     2391 N959~
    ##  7  2013     3    17  2321   810   911   135  1020   915 DL     2119 N927~
    ##  8  2013     6    27   959  1900   899  1236  2226   850 DL     2007 N376~
    ##  9  2013     7    22  2257   759   898   121  1026   895 DL     2047 N671~
    ## 10  2013    12     5   756  1700   896  1058  2020   878 AA      172 N5DM~
    ## # ... with more rows, and 7 more variables: origin <chr>, dest <chr>,
    ## #   air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>, time_hour
    ## #   <dbl>

``` r
summarise(flights, mean_dep_delay = mean(dep_delay))
```

    ## Warning: Missing values are always removed in SQL.
    ## Use `AVG(x, na.rm = TRUE)` to silence this warning

    ## # Source: lazy query [?? x 1]
    ## # Database: spark_connection
    ##   mean_dep_delay
    ##            <dbl>
    ## 1           12.6

``` r
mutate(flights, speed = distance / air_time * 60)
```

    ## # Source: lazy query [?? x 20]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    517     515   2.00   830    819  11.0  UA     1545
    ##  2  2013     1     1    533     529   4.00   850    830  20.0  UA     1714
    ##  3  2013     1     1    542     540   2.00   923    850  33.0  AA     1141
    ##  4  2013     1     1    544     545  -1.00  1004   1022 -18.0  B6      725
    ##  5  2013     1     1    554     600  -6.00   812    837 -25.0  DL      461
    ##  6  2013     1     1    554     558  -4.00   740    728  12.0  UA     1696
    ##  7  2013     1     1    555     600  -5.00   913    854  19.0  B6      507
    ##  8  2013     1     1    557     600  -3.00   709    723 -14.0  EV     5708
    ##  9  2013     1     1    557     600  -3.00   838    846 - 8.00 B6       79
    ## 10  2013     1     1    558     600  -2.00   753    745   8.00 AA      301
    ## # ... with more rows, and 9 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>, speed <dbl>

## Laziness

When working with databases, dplyr tries to be as lazy as possible:

  - It never pulls data into R unless you explicitly ask for it.

  - It delays doing any work until the last possible moment: it collects
    together everything you want to do and then sends it to the database
    in one step.

For example, take the following
code:

``` r
c1 <- filter(flights, day == 17, month == 5, carrier %in% c('UA', 'WN', 'AA', 'DL'))
c2 <- select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 <- arrange(c2, year, month, day, carrier)
c4 <- mutate(c3, air_time_hours = air_time / 60)
```

This sequence of operations never actually touches the database. It’s
not until you ask for the data (e.g. by printing `c4`) that dplyr
requests the results from the database.

``` r
c4
```

    ## # Source: lazy query [?? x 8]
    ## # Database: spark_connection
    ## # Ordered by: year, month, day, carrier
    ##     year month   day carrier dep_delay air_time distance air_time_hours
    ##    <int> <int> <int> <chr>       <dbl>    <dbl>    <dbl>          <dbl>
    ##  1  2013     5    17 AA          -2.00      294     2248           4.90
    ##  2  2013     5    17 AA          -1.00      146     1096           2.43
    ##  3  2013     5    17 AA          -2.00      185     1372           3.08
    ##  4  2013     5    17 AA          -9.00      186     1389           3.10
    ##  5  2013     5    17 AA           2.00      147     1096           2.45
    ##  6  2013     5    17 AA          -4.00      114      733           1.90
    ##  7  2013     5    17 AA          -7.00      117      733           1.95
    ##  8  2013     5    17 AA          -7.00      142     1089           2.37
    ##  9  2013     5    17 AA          -6.00      148     1089           2.47
    ## 10  2013     5    17 AA          -7.00      137      944           2.28
    ## # ... with more rows

## Piping

You can use
[magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)
pipes to write cleaner syntax. Using the same example from above, you
can write a much cleaner version like this:

``` r
c4 <- flights %>%
  filter(month == 5, day == 17, carrier %in% c('UA', 'WN', 'AA', 'DL')) %>%
  select(carrier, dep_delay, air_time, distance) %>%
  arrange(carrier) %>%
  mutate(air_time_hours = air_time / 60)
```

## Grouping

The `group_by` function corresponds to the `GROUP BY` statement in SQL.

``` r
c4 %>%
  group_by(carrier) %>%
  summarize(count = n(), mean_dep_delay = mean(dep_delay))
```

    ## Warning: Missing values are always removed in SQL.
    ## Use `AVG(x, na.rm = TRUE)` to silence this warning

    ## # Source: lazy query [?? x 3]
    ## # Database: spark_connection
    ##   carrier count mean_dep_delay
    ##   <chr>   <dbl>          <dbl>
    ## 1 AA       94.0           1.47
    ## 2 DL      136             6.24
    ## 3 UA      172             9.63
    ## 4 WN       34.0           7.97

## Collecting to R

You can copy data from Spark into R’s memory by using `collect()`.

``` r
carrierhours <- collect(c4)
```

`collect()` executes the Spark query and returns the results to R for
further analysis and visualization.

``` r
# Test the significance of pairwise differences and plot the results
with(carrierhours, pairwise.t.test(air_time, carrier))
```

    ## 
    ##  Pairwise comparisons using t tests with pooled SD 
    ## 
    ## data:  air_time and carrier 
    ## 
    ##    AA      DL      UA     
    ## DL 0.25057 -       -      
    ## UA 0.07957 0.00044 -      
    ## WN 0.07957 0.23488 0.00041
    ## 
    ## P value adjustment method: holm

``` r
ggplot(carrierhours, aes(carrier, air_time_hours)) + geom_boxplot()
```

![](guides-dplyr_files/figure-gfm/unnamed-chunk-12-1.png)<!-- -->

## SQL Translation

It’s relatively straightforward to translate R code to SQL (or indeed to
any programming language) when doing simple mathematical operations of
the form you normally use when filtering, mutating and summarizing.
dplyr knows how to convert the following R functions to Spark SQL:

``` r
# Basic math operators
+, -, *, /, %%, ^
  
# Math functions
abs, acos, asin, asinh, atan, atan2, ceiling, cos, cosh, exp, floor, log, log10, round, sign, sin, sinh, sqrt, tan, tanh

# Logical comparisons
<, <=, !=, >=, >, ==, %in%

# Boolean operations
&, &&, |, ||, !

# Character functions
paste, tolower, toupper, nchar

# Casting
as.double, as.integer, as.logical, as.character, as.date

# Basic aggregations
mean, sum, min, max, sd, var, cor, cov, n
```

## Window Functions

dplyr supports Spark SQL window functions. Window functions are used in
conjunction with mutate and filter to solve a wide range of problems.
You can compare the dplyr syntax to the query it has generated by using
`dbplyr::sql_render()`.

``` r
# Find the most and least delayed flight each day
bestworst <- flights %>%
  group_by(year, month, day) %>%
  select(dep_delay) %>% 
  filter(dep_delay == min(dep_delay) || dep_delay == max(dep_delay))
dbplyr::sql_render(bestworst)
## Warning: Missing values are always removed in SQL.
## Use `min(x, na.rm = TRUE)` to silence this warning
## Warning: Missing values are always removed in SQL.
## Use `max(x, na.rm = TRUE)` to silence this warning
## <SQL> SELECT `year`, `month`, `day`, `dep_delay`
## FROM (SELECT `year`, `month`, `day`, `dep_delay`, min(`dep_delay`) OVER (PARTITION BY `year`, `month`, `day`) AS `zzz3`, max(`dep_delay`) OVER (PARTITION BY `year`, `month`, `day`) AS `zzz4`
## FROM (SELECT `year`, `month`, `day`, `dep_delay`
## FROM `flights`) `coaxmtqqbj`) `efznnpuovy`
## WHERE (`dep_delay` = `zzz3` OR `dep_delay` = `zzz4`)
bestworst
## Warning: Missing values are always removed in SQL.
## Use `min(x, na.rm = TRUE)` to silence this warning

## Warning: Missing values are always removed in SQL.
## Use `max(x, na.rm = TRUE)` to silence this warning
## # Source: lazy query [?? x 4]
## # Database: spark_connection
## # Groups: year, month, day
##     year month   day dep_delay
##    <int> <int> <int>     <dbl>
##  1  2013     1     1     853  
##  2  2013     1     1   -  15.0
##  3  2013     1     1   -  15.0
##  4  2013     1     9    1301  
##  5  2013     1     9   -  17.0
##  6  2013     1    24   -  15.0
##  7  2013     1    24     329  
##  8  2013     1    29   -  27.0
##  9  2013     1    29     235  
## 10  2013     2     1   -  15.0
## # ... with more rows
```

``` r
# Rank each flight within a daily
ranked <- flights %>%
  group_by(year, month, day) %>%
  select(dep_delay) %>% 
  mutate(rank = rank(desc(dep_delay)))
dbplyr::sql_render(ranked)
```

    ## <SQL> SELECT `year`, `month`, `day`, `dep_delay`, rank() OVER (PARTITION BY `year`, `month`, `day` ORDER BY `dep_delay` DESC) AS `rank`
    ## FROM (SELECT `year`, `month`, `day`, `dep_delay`
    ## FROM `flights`) `mauqwkxuam`

``` r
ranked
```

    ## # Source: lazy query [?? x 5]
    ## # Database: spark_connection
    ## # Groups: year, month, day
    ##     year month   day dep_delay  rank
    ##    <int> <int> <int>     <dbl> <int>
    ##  1  2013     1     1       853     1
    ##  2  2013     1     1       379     2
    ##  3  2013     1     1       290     3
    ##  4  2013     1     1       285     4
    ##  5  2013     1     1       260     5
    ##  6  2013     1     1       255     6
    ##  7  2013     1     1       216     7
    ##  8  2013     1     1       192     8
    ##  9  2013     1     1       157     9
    ## 10  2013     1     1       155    10
    ## # ... with more rows

## Peforming Joins

It’s rare that a data analysis involves only a single table of data. In
practice, you’ll normally have many tables that contribute to an
analysis, and you need flexible tools to combine them. In dplyr, there
are three families of verbs that work with two tables at a time:

  - Mutating joins, which add new variables to one table from matching
    rows in another.

  - Filtering joins, which filter observations from one table based on
    whether or not they match an observation in the other table.

  - Set operations, which combine the observations in the data sets as
    if they were set elements.

All two-table verbs work similarly. The first two arguments are `x` and
`y`, and provide the tables to combine. The output is always a new table
with the same type as `x`.

The following statements are equivalent:

``` r
flights %>% left_join(airlines)
```

    ## Joining, by = "carrier"

    ## # Source: lazy query [?? x 20]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    517     515   2.00   830    819  11.0  UA     1545
    ##  2  2013     1     1    533     529   4.00   850    830  20.0  UA     1714
    ##  3  2013     1     1    542     540   2.00   923    850  33.0  AA     1141
    ##  4  2013     1     1    544     545  -1.00  1004   1022 -18.0  B6      725
    ##  5  2013     1     1    554     600  -6.00   812    837 -25.0  DL      461
    ##  6  2013     1     1    554     558  -4.00   740    728  12.0  UA     1696
    ##  7  2013     1     1    555     600  -5.00   913    854  19.0  B6      507
    ##  8  2013     1     1    557     600  -3.00   709    723 -14.0  EV     5708
    ##  9  2013     1     1    557     600  -3.00   838    846 - 8.00 B6       79
    ## 10  2013     1     1    558     600  -2.00   753    745   8.00 AA      301
    ## # ... with more rows, and 9 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>, name <chr>

``` r
flights %>% left_join(airlines, by = "carrier")
```

    ## # Source: lazy query [?? x 20]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    517     515   2.00   830    819  11.0  UA     1545
    ##  2  2013     1     1    533     529   4.00   850    830  20.0  UA     1714
    ##  3  2013     1     1    542     540   2.00   923    850  33.0  AA     1141
    ##  4  2013     1     1    544     545  -1.00  1004   1022 -18.0  B6      725
    ##  5  2013     1     1    554     600  -6.00   812    837 -25.0  DL      461
    ##  6  2013     1     1    554     558  -4.00   740    728  12.0  UA     1696
    ##  7  2013     1     1    555     600  -5.00   913    854  19.0  B6      507
    ##  8  2013     1     1    557     600  -3.00   709    723 -14.0  EV     5708
    ##  9  2013     1     1    557     600  -3.00   838    846 - 8.00 B6       79
    ## 10  2013     1     1    558     600  -2.00   753    745   8.00 AA      301
    ## # ... with more rows, and 9 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>, name <chr>

``` r
flights %>% left_join(airlines, by = c("carrier", "carrier"))
```

    ## # Source: lazy query [?? x 20]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    517     515   2.00   830    819  11.0  UA     1545
    ##  2  2013     1     1    533     529   4.00   850    830  20.0  UA     1714
    ##  3  2013     1     1    542     540   2.00   923    850  33.0  AA     1141
    ##  4  2013     1     1    544     545  -1.00  1004   1022 -18.0  B6      725
    ##  5  2013     1     1    554     600  -6.00   812    837 -25.0  DL      461
    ##  6  2013     1     1    554     558  -4.00   740    728  12.0  UA     1696
    ##  7  2013     1     1    555     600  -5.00   913    854  19.0  B6      507
    ##  8  2013     1     1    557     600  -3.00   709    723 -14.0  EV     5708
    ##  9  2013     1     1    557     600  -3.00   838    846 - 8.00 B6       79
    ## 10  2013     1     1    558     600  -2.00   753    745   8.00 AA      301
    ## # ... with more rows, and 9 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>, name <chr>

## Sampling

You can use `sample_n()` and `sample_frac()` to take a random sample of
rows: use `sample_n()` for a fixed number and `sample_frac()` for a
fixed fraction.

``` r
sample_n(flights, 10)
```

    ## # Source: lazy query [?? x 19]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    517     515   2.00   830    819  11.0  UA     1545
    ##  2  2013     1     1    533     529   4.00   850    830  20.0  UA     1714
    ##  3  2013     1     1    542     540   2.00   923    850  33.0  AA     1141
    ##  4  2013     1     1    544     545  -1.00  1004   1022 -18.0  B6      725
    ##  5  2013     1     1    554     600  -6.00   812    837 -25.0  DL      461
    ##  6  2013     1     1    554     558  -4.00   740    728  12.0  UA     1696
    ##  7  2013     1     1    555     600  -5.00   913    854  19.0  B6      507
    ##  8  2013     1     1    557     600  -3.00   709    723 -14.0  EV     5708
    ##  9  2013     1     1    557     600  -3.00   838    846 - 8.00 B6       79
    ## 10  2013     1     1    558     600  -2.00   753    745   8.00 AA      301
    ## # ... with more rows, and 8 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>

``` r
sample_frac(flights, 0.01)
```

    ## # Source: lazy query [?? x 19]
    ## # Database: spark_connection
    ##     year month   day dep_t~ sched_~ dep_d~ arr_~ sched~ arr_d~ carr~ flig~
    ##    <int> <int> <int>  <int>   <int>  <dbl> <int>  <int>  <dbl> <chr> <int>
    ##  1  2013     1     1    655     655   0     1021   1030 - 9.00 DL     1415
    ##  2  2013     1     1    656     700 - 4.00   854    850   4.00 AA      305
    ##  3  2013     1     1   1044    1045 - 1.00  1231   1212  19.0  EV     4322
    ##  4  2013     1     1   1056    1059 - 3.00  1203   1209 - 6.00 EV     4479
    ##  5  2013     1     1   1317    1325 - 8.00  1454   1505 -11.0  MQ     4475
    ##  6  2013     1     1   1708    1700   8.00  2037   2005  32.0  WN     1066
    ##  7  2013     1     1   1825    1829 - 4.00  2056   2053   3.00 9E     3286
    ##  8  2013     1     1   1843    1845 - 2.00  1955   2024 -29.0  DL      904
    ##  9  2013     1     1   2108    2057  11.0     25     39 -14.0  UA     1517
    ## 10  2013     1     2    557     605 - 8.00   832    823   9.00 DL      544
    ## # ... with more rows, and 8 more variables: tailnum <chr>, origin <chr>,
    ## #   dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    ## #   time_hour <dbl>

## Writing Data

It is often useful to save the results of your analysis or the tables
that you have generated on your Spark cluster into persistent storage.
The best option in many scenarios is to write the table out to a
[Parquet](https://parquet.apache.org/) file using the
[spark\_write\_parquet](reference/sparklyr/spark_write_parquet.html)
function. For example:

``` r
spark_write_parquet(tbl, "hdfs://hdfs.company.org:9000/hdfs-path/data")
```

This will write the Spark DataFrame referenced by the tbl R variable to
the given HDFS path. You can use the
[spark\_read\_parquet](reference/sparklyr/spark_read_parquet.html)
function to read the same table back into a subsequent Spark
session:

``` r
tbl <- spark_read_parquet(sc, "data", "hdfs://hdfs.company.org:9000/hdfs-path/data")
```

You can also write data as CSV or JSON using the
[spark\_write\_csv](reference/sparklyr/spark_write_csv.html) and
[spark\_write\_json](reference/sparklyr/spark_write_json.html)
functions.

## Hive Functions

Many of Hive’s built-in functions (UDF) and built-in aggregate functions
(UDAF) can be called inside dplyr’s mutate and summarize. The [Languange
Reference
UDF](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF)
page provides the list of available functions.

The following example uses the **datediff** and **current\_date** Hive
UDFs to figure the difference between the flight\_date and the current
system date:

``` r
flights %>% 
  mutate(flight_date = paste(year,month,day,sep="-"),
         days_since = datediff(current_date(), flight_date)) %>%
  group_by(flight_date,days_since) %>%
  tally() %>%
  arrange(-days_since)
```

    ## # Source: lazy query [?? x 3]
    ## # Database: spark_connection
    ## # Groups: flight_date
    ## # Ordered by: -days_since
    ##    flight_date days_since     n
    ##    <chr>            <int> <dbl>
    ##  1 2013-1-1          1844   842
    ##  2 2013-1-2          1843   943
    ##  3 2013-1-3          1842   914
    ##  4 2013-1-4          1841   915
    ##  5 2013-1-5          1840   720
    ##  6 2013-1-6          1839   832
    ##  7 2013-1-7          1838   933
    ##  8 2013-1-8          1837   899
    ##  9 2013-1-9          1836   902
    ## 10 2013-1-10         1835   932
    ## # ... with more rows
