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

test_that("ml_clustering_evalator() can be used in a CV pipeline", {
  test_requires_version("2.3.0", "ml clustering evaluator requires spark 2.3+")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  pipeline_cv <- ml_pipeline(sc) %>%
    ft_vector_assembler(
      input_cols = c("Sepal_Width", "Sepal_Length", "Petal_Width", "Petal_Length"),
      output_col = "features"
    ) %>%
    ml_kmeans(uid = "kmeans")

  # Specify hyperparameter grid
  grid <- list(
    kmeans = list(
      k = c(2, 3, 4)
    )
  )

  # Create the cross validator object
  cv <- ml_cross_validator(
    sc,
    estimator = pipeline_cv,
    estimator_param_maps = grid,
    evaluator = ml_clustering_evaluator(sc),
    num_folds = 3,
    parallelism = 1
  )

  # Train the models
  expect_error(
    ml_fit(cv, iris_tbl),
    NA
  )
})
