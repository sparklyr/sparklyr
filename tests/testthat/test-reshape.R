context("reshape")

sc <- testthat_spark_connection()

test_that("'gather()' works on tbl_sparks", {
  skip_on_cran()
  skip_if_not_installed("tidyr")
  library(tidyr)

  stocks <- data_frame(
    id   = letters[1:10],
    X    = rnorm(10, 0, 1),
    Y    = rnorm(10, 0, 2),
    Z    = rnorm(10, 0, 4)
  )

  stocks_tbl <- copy_to(sc, stocks, overwrite = TRUE)

  R <-
    gather(stocks, stock, price, X:Z) %>%
    collect() %>%
    arrange(id, stock)

  S <-
    gather(stocks_tbl, stock, price, X:Z) %>%
    collect() %>%
    arrange(id, stock)

  # NOTE: all.equal seems to demand equivalece
  # for numeric columns here?
  invisible(lapply(seq_along(R), function(i) {
    expect_equivalent(R[[i]], S[[i]])
  }))

})
