context("reshape")

sc <- testthat_spark_connection()

test_that("'gather()' works on tbl_sparks", {

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

  invisible(lapply(seq_along(R), function(i) {
    expect_equivalent(R[[i]], S[[i]])
  }))

})
