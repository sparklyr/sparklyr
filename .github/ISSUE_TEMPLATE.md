# Reporting an Issue with sparklyr

For general programming questions with `sparklyr`, please ask on
[Stack Overflow](http://stackoverflow.com) instead.

Please briefly describe your problem and, when relevant, the output you expect.
Please also provide the output of `utils::sessionInfo()` or
`devtools::session_info()` at the end of your post.

If at all possible, please include a [minimal, reproducible
example](https://stackoverflow.com/questions/5963269/how-to-make-a-great-r-reproducible-example).
The `sparklyr` maintainer and the broader open-source community will be much more likely to assist
with your issue if they are able to reproduce it themselves locally. If possible, try generating
a reproducible example using the [reprex](https://github.com/tidyverse/reprex) package.

Please delete this preamble after you have read it.

---

your brief description of the problem

```r
spark_version <- "2.1.0"
sc <- spark_connect(master = "local", version = spark_version)

# your reproducible example here
```

