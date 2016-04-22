Spark
=======

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

tbl(db, "flights") %>% collect

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
