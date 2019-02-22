sparklyr: R interface for Apache Spark
================

[![Build
Status](https://travis-ci.org/rstudio/sparklyr.svg?branch=master)](https://travis-ci.org/rstudio/sparklyr)
[![AppVeyor Build
Status](https://ci.appveyor.com/api/projects/status/github/rstudio/sparklyr?branch=master&svg=true)](https://ci.appveyor.com/project/JavierLuraschi/sparklyr)
[![CRAN\_Status\_Badge](https://www.r-pkg.org/badges/version/sparklyr)](https://cran.r-project.org/package=sparklyr)
<a href="https://www.r-pkg.org/pkg/sparklyr"><img src="https://cranlogs.r-pkg.org/badges/sparklyr?color=brightgreen" style=""></a>
[![codecov](https://codecov.io/gh/rstudio/sparklyr/branch/master/graph/badge.svg)](https://codecov.io/gh/rstudio/sparklyr)
[![Join the chat at
https://gitter.im/rstudio/sparklyr](https://badges.gitter.im/rstudio/sparklyr.svg)](https://gitter.im/rstudio/sparklyr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="tools/readme/sparklyr-illustration.png" width="320" align="right" style="margin-left: 20px; margin-right: 20px"/>

  - Install and connect to [Spark](http://spark.apache.org/) using YARN,
    Mesos, Livy or Kubernetes.
  - Use [dplyr](https://spark.rstudio.com/dplyr/) to filter and
    aggregate Spark datasets and
    [streams](https://spark.rstudio.com/guides/streaming/) then bring
    them into R for analysis and visualization.
  - Use [MLlib](http://spark.apache.org/docs/latest/mllib-guide.html),
    [H2O](https://spark.rstudio.com/guides/h2o/),
    [XGBoost](https://github.com/rstudio/sparkxgb) and
    [GraphFrames](https://github.com/rstudio/graphframes) to train
    models at scale in Spark.
  - Create interoperable machine learning
    [pipelines](https://spark.rstudio.com/guides/pipelines/) and
    productionize them with
    [MLeap](https://spark.rstudio.com/guides/mleap/).
  - Create [extensions](http://spark.rstudio.com/extensions.html) that
    call the full Spark API or run [distributed
    R](https://spark.rstudio.com/guides/distributed-r/) code to support
    new functionality.

## Installation

You can install the **sparklyr** package from
[CRAN](https://CRAN.r-project.org) as follows:
