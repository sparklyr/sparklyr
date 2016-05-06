Spark
=======

[![Travis-CI Build Status](https://travis-ci.com/rstudio/spark.svg?branch=master)](https://travis-ci.com/rstudio/spark)

A set of tools to provision, connect and interface to Apache Spark from within the
R language and echosystem. This package supports connecting to local and remote
Apache Spark clusters and provides support for R packages like dplyr and DBI.

## Installation

Spark is not yet on CRAN; it is currently available from GitHub. Install the devtools package followed by:

```R
devtools::install_github("rstudio/spark")
```

## Examples

### Local

This example starts a new Spark local instance and performs basic table operations:

```R
library(spark)
library(dplyr)
library(nycflights13)

db <- src_spark()
copy_to(db, flights, "flights")
src_tbls(db)

tbl(db, "flights") %>% filter(dep_delay == 2) %>% head

```

### Remote

To start a new 1-master 1-slave Spark cluster in EC2 run the following code:

```R
library(spark)
start_ec2(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
          secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
          pem_file = "sparkster.pem")
```

The `access_key_id`, `secret_access_key` and `pem_file` need to be retrieved from the AWS console.

### Further Reading

[Introduction to dplyr](https://cran.rstudio.com/web/packages/dplyr/vignettes/introduction.html) provides additional dplyr examples that can be used over dplyr with minimal modifications. For example, consider the last example from the tutorial which would be represented in spark and dplyr as follows:


```
library(spark)
library(dplyr)
library(nycflights13)
library(ggplot2)

db <- src_spark()
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
