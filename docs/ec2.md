Using RSpark in EC2
================

Deployment
----------

Deploy a 1-master 1-worker cluster using:

``` r
ci <- spark_ec2_cluster(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
                        secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                        pem_file = "spark.pem")
spark_ec2_deploy(ci)
```

Remote connect to the master node by running the following command and running the result in a terminal window:

``` r
spark_ec2_login(ci)
```

Once connected, change the password using:

`passwd rstudio`

Back from R, launch R studio in the cluster machine using:

``` r
spark_ec2_rstudio(ci)
```

Compilation
-----------

Using the remote terminal into the cluster, run:

    yum install openssl-devel
    yum -y install libcurl libcurl-devel

Then back from R install:

``` r
install.packages("devtools")

devtools::install_github("hadley/dplyr")
devtools::install_github("nycflights13")
```

Finally, build the RSpark package.

Connection
----------

Once all packages have compiled, connect to the EC2 cluster using:

``` r
library(dplyr)

master <- system('cat /root/spark-ec2/cluster-url', intern=TRUE)
sc <- spark_connect(master)
db <- src_spark(sc)

src_tbls(db)
```
