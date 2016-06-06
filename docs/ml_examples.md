RSpark ML: Examples
================

KMeans in R
-----------

``` r
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
library(ggplot2)
```

    ## Warning: package 'ggplot2' was built under R version 3.2.4

``` r
cl <- iris %>%
  select(Petal.Width, Petal.Length) %>%
  kmeans(3)

centers <- as.data.frame(cl$centers)

iris %>%
  select(Petal.Width, Petal.Length) %>%
  ggplot(aes(x=Petal.Length, y=Petal.Width)) +
    geom_point(data=centers, aes(x=Petal.Width,y=Petal.Length), size=60, alpha=0.1) +
    geom_point(data=iris, aes(x=Petal.Width,y=Petal.Length), size=2, alpha=0.5)
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-1-1.png)

KMeans in RSpark
----------------

Basing kmeans over Spark on [spark.mllib K-means](http://spark.apache.org/docs/latest/mllib-clustering.html#k-means)

``` r
library(rspark)
library(dplyr)
library(ggplot2)

sc <- spark_connect("local", cores = "auto", version = "2.0.0-preview")
db <- src_spark(sc)

# copy the iris table to Spark
copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")

model <- iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  ml_kmeans(3)

iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  collect %>%
  ggplot(aes(x=Petal_Length, y=Petal_Width)) +
    geom_point(data=model$centers, aes(x=Petal_Width,y=Petal_Length), size=60, alpha=0.1) +
    geom_point(aes(x=Petal_Width,y=Petal_Length), size=2, alpha=0.5)
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-2-1.png)

``` r
spark_disconnect(sc)
```
