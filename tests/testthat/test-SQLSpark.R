library(testthat)

test_that("window function is found on expression", function() {
  spark_dplyr_any_expression(quote(1 + 2), function(e) {
    as.character(e) == "min_rank"
  })
})
