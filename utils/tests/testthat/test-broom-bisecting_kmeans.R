skip_connection("broom-bisecting_kmeans")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("bisecting_kmeans.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  mtcars_tbl <- testthat_tbl("mtcars")

  bk_model <- mtcars_tbl %>%
    ml_bisecting_kmeans(~ mpg + cyl, k = 4L, seed = 123)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(bk_model)

  check_tidy(td1,
    exp.row = 4,
    exp.names = c("mpg", "cyl", "size", "cluster")
  )

  expect_equal(td1$size, c(11, 9, 7, 5))

  ## --------------------------- augment() -------------------------------------

  au1 <- bk_model %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1,
    exp.row = nrow(mtcars),
    exp.name = c(names(mtcars), ".cluster")
  )

  au2 <- bk_model %>%
    augment(newdata = head(mtcars_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au2,
    exp.row = 25,
    exp.name = c(names(mtcars), ".cluster")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(bk_model)

  if (spark_version(sc) >= "2.3.0") {
    check_tidy(gl1,
      exp.row = 1,
      exp.names = c("k", "wssse", "silhouette")
    )
  } else {
    check_tidy(gl1,
      exp.row = 1,
      exp.names = c("k", "wssse")
    )
  }
})
