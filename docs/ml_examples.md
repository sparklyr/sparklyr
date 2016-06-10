RSpark ML: Examples
================

Initialization
--------------

``` r
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
library(ggplot2)

sc <- spark_connect("local", cores = "auto", version = "2.0.0-preview")
db <- src_spark(sc)

copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")
```

KMeans in R
-----------

``` r
cl <- iris %>%
  select(Petal.Width, Petal.Length) %>%
  kmeans(centers = 3)

centers <- as.data.frame(cl$centers)

iris %>%
  select(Petal.Width, Petal.Length) %>%
  ggplot(aes(Petal.Length, Petal.Width)) +
    geom_point(data = centers, aes(Petal.Width, Petal.Length), size = 60, alpha = 0.1) +
    geom_point(data = iris, aes(Petal.Width, Petal.Length), size = 2, alpha = 0.5)
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-2-1.png)

KMeans in RSpark
----------------

Basing kmeans over Spark on [spark.mllib K-means](http://spark.apache.org/docs/latest/mllib-clustering.html#k-means)

Note that the names of variables within the iris `tbl` have been transformed (replacing `.` with `_`) to work around an issue in the Spark 2.0.0-preview used in constructing this document -- we expect the issue to be resolved with the release of Spark 2.0.0.

``` r
model <- tbl(db, "iris") %>%
  select(Petal_Width, Petal_Length) %>%
  ml_kmeans(centers = 3)

tbl(db, "iris") %>%
  select(Petal_Width, Petal_Length) %>%
  collect %>%
  ggplot(aes(Petal_Length, Petal_Width)) +
    geom_point(data = model$centers, aes(Petal_Width, Petal_Length), size = 60, alpha = 0.1) +
    geom_point(aes(Petal_Width, Petal_Length), size = 2, alpha = 0.5)
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-3-1.png)

Linear Regression in R
----------------------

``` r
data <- iris %>%
  select(Petal.Width, Petal.Length)

model <- lm(Petal.Length ~ Petal.Width, data = iris)

iris %>%
  select(Petal.Width, Petal.Length) %>%
  ggplot(aes(Petal.Length, Petal.Width)) +
    geom_point(data = iris, aes(Petal.Width, Petal.Length), size = 2, alpha = 0.5) +
    geom_abline(aes(slope = coef(model)[["Petal.Width"]],
                    intercept = coef(model)[["(Intercept)"]],
                    color = "red"))
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-4-1.png)

Linear Regression in RSpark
---------------------------

``` r
model <- tbl(db, "iris") %>%
  select(Petal_Width, Petal_Length) %>%
  ml_linear_regression(response = "Petal_Length", features = c("Petal_Width"))

iris %>%
  select(Petal.Width, Petal.Length) %>%
  ggplot(aes(Petal.Length, Petal.Width)) +
    geom_point(data = iris, aes(Petal.Width, Petal.Length), size = 2, alpha = 0.5) +
    geom_abline(aes(slope = coef(model)[["Petal_Width"]],
                    intercept = coef(model)[["(Intercept)"]],
                    color = "red"))
```

![](ml_examples_files/figure-markdown_github/unnamed-chunk-5-1.png)

Partitioning in R
-----------------

``` r
ratio <- 0.75
trainingSize <- floor(ratio * nrow(iris))
indices <- sample(seq_len(nrow(iris)), size = trainingSize)

training <- iris[ indices, ]
test     <- iris[-indices, ]

fit <- lm(Petal.Length ~ Petal.Width, data = iris)
predict(fit, newdata = test)
```

    ##       11       12       14       16       26       29       33       37 
    ## 1.529546 1.529546 1.306552 1.975534 1.529546 1.529546 1.306552 1.529546 
    ##       38       39       44       45       46       48       55       57 
    ## 1.306552 1.529546 2.421522 1.975534 1.752540 1.529546 4.428469 4.651463 
    ##       62       64       67       72       75       77       82       88 
    ## 4.428469 4.205475 4.428469 3.982481 3.982481 4.205475 3.313499 3.982481 
    ##       94       95       98      104      105      118      121      124 
    ## 3.313499 3.982481 3.982481 5.097451 5.989427 5.989427 6.212421 5.097451 
    ##      133      134      135      136      138      150 
    ## 5.989427 4.428469 4.205475 6.212421 5.097451 5.097451

Partitioning in RSpark
----------------------

``` r
partitions <- tbl(db, "iris") %>%
  partition(training = 0.75, test = 0.25)

fit <- partitions$training %>%
  ml_linear_regression(response = "Petal_Length", features = c("Petal_Width"))

predict(fit, partitions$test)
```

    ##  [1] 1.348732 1.567477 1.567477 1.348732 1.786222 1.567477 1.348732
    ##  [8] 3.317439 1.567477 1.567477 1.567477 2.442458 2.004967 1.567477
    ## [15] 1.348732 1.567477 2.004967 3.536184 3.973674 3.754929 3.317439
    ## [22] 4.629910 4.411165 3.973674 3.973674 5.067401 5.723636 6.161127
    ## [29] 6.598617 6.161127 5.942381 5.504891

Principal Components Analysis in R
----------------------------------

``` r
model <- iris %>%
  select(-Species) %>%
  prcomp()
print(model)
```

    ## Standard deviations:
    ## [1] 2.0562689 0.4926162 0.2796596 0.1543862
    ## 
    ## Rotation:
    ##                      PC1         PC2         PC3        PC4
    ## Sepal.Length  0.36138659 -0.65658877  0.58202985  0.3154872
    ## Sepal.Width  -0.08452251 -0.73016143 -0.59791083 -0.3197231
    ## Petal.Length  0.85667061  0.17337266 -0.07623608 -0.4798390
    ## Petal.Width   0.35828920  0.07548102 -0.54583143  0.7536574

``` r
# calculate explained variance
model$sdev^2 / sum(model$sdev^2)
```

    ## [1] 0.924618723 0.053066483 0.017102610 0.005212184

Principal Components Analysis in RSpark
---------------------------------------

``` r
model <- tbl(db, "iris") %>%
  select(-Species) %>%
  ml_pca()
print(model)
```

    ## Explained variance:
    ##         PC1         PC2         PC3         PC4 
    ## 0.924618723 0.053066483 0.017102610 0.005212184 
    ## 
    ## Rotation:
    ##                      PC1         PC2         PC3        PC4
    ## Sepal_Length -0.36138659 -0.65658877  0.58202985  0.3154872
    ## Sepal_Width   0.08452251 -0.73016143 -0.59791083 -0.3197231
    ## Petal_Length -0.85667061  0.17337266 -0.07623608 -0.4798390
    ## Petal_Width  -0.35828920  0.07548102 -0.54583143  0.7536574

Cleanup
-------

``` r
spark_disconnect(sc)
```
