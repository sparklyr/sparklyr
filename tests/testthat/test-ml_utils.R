skip_connection("ml_utils")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("make_stats_arranger reorders for intercept or is identity", {
  expect_identical(make_stats_arranger(TRUE)(c(1, 2, 3)), c(3, 1, 2))
  expect_identical(make_stats_arranger(FALSE)(c(1, 2, 3)), c(1, 2, 3))
})

test_that("possibly_null swallows errors and otherwise returns the value", {
  expect_null(possibly_null(function(x) stop("boom"))(1))
  expect_equal(possibly_null(function(x) x + 1)(1), 2)
})

test_that("residuals / ml_feature_importances reject unsupported models", {
  expect_error(residuals(structure(list(), class = "ml_model")), "not supported")
  expect_error(
    ml_feature_importances(
      structure(list(), class = c("ml_foo_model", "ml_prediction_model"))
    ),
    "Cannot extract"
  )
  expect_error(
    ml_feature_importances(
      structure(list(), class = c("ml_model_foo", "ml_model"))
    ),
    "not supported"
  )
})

test_that("param_min_version errors below min, nulls a default, passes otherwise", {
  expect_error(
    param_min_version(sc, "frequencyAsc", "99.0.0", "frequencyDesc"),
    "only available for Spark"
  )
  expect_null(param_min_version(sc, "frequencyDesc", "99.0.0", "frequencyDesc"))
  expect_equal(param_min_version(sc, "x", NULL), "x")
})

test_that("spark linalg helpers round-trip vectors/matrices and build columns", {
  v <- spark_dense_vector(sc, c(1, 2, 3))
  expect_equal(as.numeric(invoke(v, "toArray")), c(1, 2, 3))
  expect_null(spark_dense_vector(sc, NULL))

  m <- spark_dense_matrix(sc, matrix(c(1, 2, 3, 4), 2, 2))
  expect_equal(dim(read_spark_matrix(m)), c(2L, 2L))
  expect_null(spark_dense_matrix(sc, NULL))

  expect_true(inherits(spark_sql_column(sc, "x", "x_alias"), "spark_jobj"))
})

test_that("read_spark_vector + ml_short_type work against fitted models", {
  feat <- sdf_copy_to(sc, iris, "mlu_iris", overwrite = TRUE) %>%
    ft_vector_assembler(c("Petal_Length", "Petal_Width"), "features")
  scaler <- ml_fit(
    ft_standard_scaler(sc, "features", "scaled", with_std = TRUE),
    feat
  )
  expect_true(is.numeric(read_spark_vector(spark_jobj(scaler), "std")))
  expect_match(ml_short_type(ml_kmeans(sc, k = 2)), "KMeans")
})

test_that("ml_feature_importances returns importances for tree models", {
  feat <- sdf_copy_to(sc, iris, "mlu_iris2", overwrite = TRUE) %>%
    ft_vector_assembler(c("Petal_Length", "Petal_Width"), "features")
  # prediction-model path -> numeric vector
  pm <- ml_fit(
    ml_gbt_regressor(sc, features_col = "features", label_col = "Sepal_Length"),
    feat
  )
  expect_true(is.numeric(ml_feature_importances(pm)))
  # ml_model (formula) path -> sorted data frame
  fm <- testthat_tbl("iris") %>%
    ml_gbt_regressor(Sepal_Length ~ Petal_Length + Petal_Width)
  fi <- ml_feature_importances(fm)
  expect_s3_class(fi, "data.frame")
  expect_setequal(names(fi), c("feature", "importance"))
})

test_that("predict/fitted/ml_model_data and the post_ml_obj dispatchers work", {
  iris_tbl <- testthat_tbl("iris")
  fm_reg <- iris_tbl %>%
    ml_gbt_regressor(Sepal_Length ~ Petal_Length + Petal_Width)
  expect_length(predict(fm_reg), 150) # predict.ml_model_regression
  expect_length(fitted(fm_reg), 150) # fitted.ml_model_prediction
  expect_s3_class(ml_model_data(fm_reg), "tbl_spark")
  expect_s3_class(ml_tree_feature_importance(fm_reg), "data.frame")

  bin <- iris_tbl %>% dplyr::mutate(lab = ifelse(Species == "setosa", "yes", "no"))
  fm_cls <- bin %>% ml_gbt_classifier(lab ~ Petal_Length + Petal_Width)
  expect_length(predict(fm_cls), 150) # predict.ml_model_classification

  # post_ml_obj dispatchers: ml_pipeline (add stage) and tbl_spark (no formula -> fit)
  expect_s3_class(
    ml_gbt_regressor(ml_pipeline(testthat_spark_connection()), Sepal_Length ~ Petal_Length),
    "ml_pipeline"
  )
  feat <- iris_tbl %>% ft_vector_assembler(c("Petal_Length", "Petal_Width"), "features")
  expect_s3_class(
    ml_gbt_regressor(feat, features_col = "features", label_col = "Sepal_Length"),
    "ml_gbt_regression_model"
  )
})

test_clear_cache()
