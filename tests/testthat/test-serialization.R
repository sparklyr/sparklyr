context("serialization")
sc <- testthat_spark_connection()

test_requires("nycflights13")
flights_tbl <- testthat_tbl("flights")

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

test_that("objects survive Spark roundtrips", {
  skip_on_cran()

  datasets <- list(mtcars = mtcars)
  for (dataset in datasets)
    ensure_round_trip(sc, dataset)
})

test_that("primitive values survive Spark roundtrips", {
  skip_on_cran()

  n <- 10
  df <- data.frame(
    int = as.integer(1:n),
    dbl = as.double(1:n),
    lgl = rep_len(c(TRUE, FALSE), n),
    chr = letters[1:10],
    stringsAsFactors = FALSE
  )

  ensure_round_trip(sc, df)

})

test_that("NA values survive Spark roundtrips", {
  skip_on_cran()

  n <- 10
  df <- data.frame(
    int = as.integer(1:n),
    dbl = as.double(1:n),
    # lgl = rep_len(c(TRUE, FALSE), n), # TODO
    # chr = letters[1:10],              # TODO
    stringsAsFactors = FALSE
  )

  df[n / 2, ] <- NA

  ensure_round_trip(sc, df)
})

test_that("data.frames with '|' can be copied", {
  skip_on_cran()

  pipes <- data.frame(
    x = c("|||", "|||", "|||"),
    y = c(1, 2, 3),
    stringsAsFactors = FALSE
  )

  ensure_round_trip(sc, pipes)
})

test_that("data.frames with many columns survive roundtrip", {
  skip_on_cran()

  n <- 1E3
  data <- as.data.frame(replicate(n, 1L, simplify = FALSE))
  names(data) <- paste("X", 1:n, sep = "")

  ensure_round_trip(sc, data)
})

test_that("data.frames with many columns don't cause Java StackOverflows", {
  skip_on_cran() && skip_unless_verbose("Skipping slow serialization test")

  n <- 5000
  df <- matrix(0, ncol = 5000, nrow = 2) %>% as_data_frame()
  sdf <- copy_to(sc, df, overwrite = TRUE)

  # the above failed with a Java StackOverflow with older versions of sparklyr
  expect_true(TRUE, info = "no Java StackOverflow on copy of large dataset")
})

test_that("'sdf_predict()', 'predict()' return same results", {

  model <- flights_tbl %>%
    ml_decision_tree(sched_dep_time ~ dep_time)

  id <- model$model.parameters$id

  predictions <- sdf_predict(model)
  n1 <- spark_dataframe(predictions) %>% invoke("count")
  n2 <- length(predict(model))

  expect_equal(n1, n2)

  lhs <- predictions %>%
    arrange_(.dots = list(model$model.parameters$id)) %>%
    sdf_read_column("prediction")

  rhs <- predict(model)

  expect_identical(lhs, rhs)

})

test_that("copy_to() succeeds when last column contains missing / empty values", {

  df <- data.frame(
    x = c(1, 2),
    z = c(NA, ""),
    stringsAsFactors = FALSE
  )

  copy_to(sc, df, serializer = "csv_string", overwrite = TRUE)

})

