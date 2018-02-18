context("ml backwards compatibility")

sc <- testthat_spark_connection()

test_that("sdf_predict() can take a transformer/model as first argument (#1287)", {
  iris_tbl <- testthat_tbl("iris")

  km <- iris_tbl %>%
    ml_kmeans(centers = 3, features = c("Sepal_Length","Sepal_Width"))

  expect_warning(
    sdf_predict(km$pipeline_model, iris_tbl),
    "The signature sdf_predict\\(transformer, dataset\\) is deprecated and will be removed in a future version. Use sdf_predict\\(dataset, transformer\\) or ml_predict\\(transformer, dataset\\) instead."
  )

  expect_warning(
    sdf_predict(km, iris_tbl),
    "The signature sdf_predict\\(model, dataset\\) is deprecated and will be removed in a future version. Use sdf_predict\\(dataset, model\\) or ml_predict\\(model, dataset\\) instead."
  )
})
