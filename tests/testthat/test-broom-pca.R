context("broom-pca")

test_that("pca.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")

  td1 <- iris_tbl %>%
    dplyr::select(-Species) %>%
    ml_pca(k = 3) %>%
    tidy()

  model <- iris %>%
    dplyr::select(-Species) %>%
    stats::prcomp()

  check_tidy(td1, exp.row = 4, exp.col = 4,
             exp.names = c("features", "PC1", "PC2", "PC3"))

  expect_equal(td1$PC1,
               -as.vector(model$rotation[,1]),
               tolerance = 0.001)

})

test_that("pca.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")


  au1 <- iris_tbl %>%
    dplyr::select(-Species) %>%
    ml_pca(k = 3) %>%
    augment(head(iris_tbl, 25)) %>%
    dplyr::collect()

  check_tidy(au1, exp.row = 25,
             exp.name = c(dplyr::tbl_vars(iris_tbl),
                          "PC1", "PC2", "PC3"))
})

test_that("pca.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  gl1 <- iris_tbl %>%
    dplyr::select(-Species) %>%
    ml_pca(k = 3)%>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("k", "explained_variance_PC1",
                           "explained_variance_PC2" ,"explained_variance_PC3"))
})
