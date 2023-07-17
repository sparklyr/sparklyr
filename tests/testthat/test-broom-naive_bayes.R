skip_connection("broom-naive_bayes")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
library(dplyr)

test_that("naive_bayes.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  nb_model <- iris_tbl %>%
    ml_naive_bayes(Species ~ Sepal_Length + Petal_Length)

  ## ----------------------------- tidy() --------------------------------------

  # for multiclass classification
  td1 <- tidy(nb_model)

  check_tidy(td1,
    exp.row = 3, exp.col = 4,
    exp.names = c(".label", "Sepal_Length", "Petal_Length", ".pi")
    )

  expect_equal(
    sort(td1$Sepal_Length),
    sort(c(-0.258, -0.542, -0.612)),
    tolerance = 0.001, scale = 1
    )

  ## --------------------------- augment() -------------------------------------

  # with newdata
  au1 <- nb_model %>%
    augment(head(iris_tbl, 25)) %>%
    collect()

  check_tidy(au1,
    exp.row = 25,
    exp.name = c(dplyr::tbl_vars(iris_tbl), ".predicted_label")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(nb_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("model_type", "smoothing")
  )
})
