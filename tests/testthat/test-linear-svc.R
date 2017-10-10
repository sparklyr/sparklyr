context("ml linear svc")

sc <- testthat_spark_connection()

test_that("ml_linear_svc param setting", {
  skip_if_not(spark_version(sc) >= "2.2.0")
  args <- list(
    x = sc, fit_intercept = FALSE, reg_param = 0.01, max_iter = 67,
    standardization = FALSE, weight_col = "wcol", tol = 1e-04,
    threshold = Inf, aggregation_depth = 3, raw_prediction_col = "rpcol",
    label_col = "col", features_col = "fcol", prediction_col = "pcol"
  )
  ovr <- do.call(ml_linear_svc, args)
  expect_equal(ml_params(ovr, names(args)[-1]), args[-1])
})

test_that("ml_linear_svc() default params are correct", {
  skip_if_not(spark_version(sc) >= "2.2.0")
  predictor <- ml_pipeline(sc) %>%
    ml_linear_svc() %>%
    ml_stage(1)

  args <- get_default_args(ml_linear_svc,
                           c("x", "uid", "...", "weight_col"))

  expect_equal(
    ml_params(predictor, names(args)),
    args)
})

test_that("ml_linear_svc() runs", {
  skip_if_not(spark_version(sc) >= "2.2.0")
  iris_tbl2 <- testthat_tbl("iris") %>%
    mutate(is_versicolor = ifelse(
      Species == "versicolor", "versicolor", "other")) %>%
    select(-Species)

  expect_error(
    ml_linear_svc(iris_tbl2, is_versicolor ~ .) %>%
      sdf_predict(iris_tbl2) %>%
      pull(predicted_label),
    NA
  )
})
