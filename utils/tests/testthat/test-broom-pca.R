skip_connection("broom-pca")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("pca tidiers work", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  pca_model <- iris_tbl %>%
    select(-Species) %>%
    ml_pca(k = 3)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(pca_model)

  model <- iris %>%
    dplyr::select(-Species) %>%
    stats::prcomp()

  check_tidy(td1,
    exp.row = 4, exp.col = 4,
    exp.names = c("features", "PC1", "PC2", "PC3")
  )

  ## --------------------------- augment() -------------------------------------

  expect_equal(td1$PC1,
    -as.vector(model$rotation[, 1]),
    tolerance = 0.001, scale = 1
  )

  au1 <- pca_model %>%
    augment(head(iris_tbl, 25)) %>%
    collect()

  check_tidy(au1,
    exp.row = 25,
    exp.name = c(
      dplyr::tbl_vars(iris_tbl),
      "PC1", "PC2", "PC3"
    )
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(pca_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c(
      "k", "explained_variance_PC1",
      "explained_variance_PC2", "explained_variance_PC3"
    )
  )
})
