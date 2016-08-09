context("Serialization")

test_that("objects survive Spark roundtrips", {
  sc <- spark_connect(master = "local[*]", version = "1.6.1")

  datasets <- list(mtcars = mtcars)

  for (dataset in datasets) {
    # round-trip data through Spark
    copied <- copy_to(sc, dataset, overwrite = TRUE)
    collected <- as.data.frame(collect(copied))

    # compare without row.names (as we don't preserve those)
    lhs <- dataset
    row.names(lhs) <- NULL
    rhs <- collected
    row.names(rhs) <- NULL

    expect_equal(unname(lhs), unname(rhs))
  }

})
