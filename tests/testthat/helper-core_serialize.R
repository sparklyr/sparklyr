ensure_round_trip <- function(sc, data) {
  # round-trip data through Spark
  copied <- copy_to(sc, data, overwrite = TRUE)
  collected <- as.data.frame(collect(copied))

  # compare without row.names (as we don't preserve those)
  lhs <- data
  row.names(lhs) <- NULL
  rhs <- collected
  row.names(rhs) <- NULL

  expect_equal(unname(lhs), unname(rhs))
}
