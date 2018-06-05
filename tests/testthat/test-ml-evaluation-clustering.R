context("ml evaluation - clustering")

test_that("ml_clustering_evaluator() works", {
  test_requires_version("2.3.0", "ml clustering evaluator requires spark 2.3+")
  sc <- testthat_spark_connection()
  sample_data_path <- dir(getwd(), recursive = TRUE, pattern = "sample_kmeans_data.txt", full.names = TRUE)
  sample_data <- spark_read_libsvm(sc, "sample_data",
                                   sample_data_path, overwrite = TRUE)
  kmeans <- ml_kmeans(sample_data, k = 2, seed = 1)
  predictions <- ml_transform(kmeans, sample_data)
  expect_equal(
    ml_clustering_evaluator(predictions),
    0.999753,
    tolerance = 1e-5
  )
})

