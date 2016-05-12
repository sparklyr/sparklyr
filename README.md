Spark
=======

[![Travis-CI Build Status](https://travis-ci.com/rstudio/spark.svg?token=MxiS2SHZy3QzqFf34wQr&branch=master)](https://travis-ci.com/rstudio/spark)

A set of tools to provision, connect and interface to Apache Spark from within the
R language and echosystem. This package supports connecting to local and remote
Apache Spark clusters and provides support for R packages like dplyr and DBI.

## Connection

### Installation

Clone this repo and build spark.Rproj

### Examples

Spark can automatically download the spark binaries and connect with ease to a local instance. Additionally, one can print the spark log entries and open the web interface as follows:

```
library(spark)

scon <- spark_connect("local")

spark_log(scon)
spark_web(scon)

spark_disconnect(scon)
```

## Dplyr

### Installation

Clone this repo and open project followed by:

```R
devtools::install_github("hadley/lazyeval")
devtools::install_github("hadley/dplyr")
```

### Examples

This example starts a new Spark local instance and performs basic table operations:

```R
library(spark)
library(dplyr)
library(nycflights13)

scon <- spark_connect("local")

db <- src_spark(scon)
copy_to(db, flights, "flights")
src_tbls(db)

tbl(db, "flights") %>% filter(dep_delay == 2) %>% head

```

### Further Reading

[Introduction to dplyr](https://cran.rstudio.com/web/packages/dplyr/vignettes/introduction.html) provides additional dplyr examples that can be used over dplyr with minimal modifications. For example, consider the last example from the tutorial which would be represented in spark and dplyr as follows:

```
library(spark)
library(dplyr)
library(nycflights13)
library(ggplot2)

scon <- spark_connect("local")

db <- src_spark(scon)
copy_to(db, flights, "flights")

delay <- tbl(db, "flights") %>% 
         group_by(tailnum) %>%
         summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
         filter(count > 20, dist < 2000) %>%
         collect
    
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area()
```

[Window functions](https://cran.r-project.org/web/packages/dplyr/vignettes/window-functions.html) provides more advanced examples that can also be used with spark. For example:

```
library(dplyr)
library(spark)
library(Lahman)

scon <- spark_connect("local")

db <- src_spark(scon)
copy_to(db, Batting, "batting")

select(tbl(db, "batting"), playerID, yearID, teamID, G, AB:H) %>%
  arrange(playerID, yearID, teamID) %>%
  group_by(playerID) %>%
  filter(min_rank(desc(H)) <= 2 & H > 0) %>%
  head
```

## Provisioning

To start a new 1-master 1-slave Spark cluster in EC2 run the following code:

```R
library(spark)
master <- start_ec2(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
                    secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                    pem_file = "sparkster.pem")
          
sc <- spark_connect(master)
spark_log(sc)
```

The `access_key_id`, `secret_access_key` and `pem_file` need to be retrieved from the AWS console.

## Extensibility

Spark provides low level access to native JVM objects, this topic targets users creating packages
based on low-level spark integration.

### Examples

```
library(spark)
library(nycflights13)

scon <- spark_connect("local")

tempfile <- tempfile(fileext = ".csv")
write.csv(flights, tempfile, row.names = FALSE, na = "")

count_lines <- function(scon, path) {
  read <- spark_invoke(scon, spark_context(scon), "textFile", path)
  spark_invoke(scon, read, "count")
}

count_lines(scon, tempfile)

spark_disconnect(scon)
```
