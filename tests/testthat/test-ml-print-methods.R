context("ml print methods")

sc <- testthat_spark_connection()

test_that("printing works for ml_model_logistic_regression", {
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))
  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  s <- ml_logistic_regression(
    iris_weighted_tbl,
    response = "versicolor",
    features = c("Sepal_Width", "Petal_Length", "Petal_Width"))

  expect_output(print(s),
                "Call: ml_logistic_regression.tbl_spark\\(")

})

test_that("ml_tree_feature_importance() works properly", {
  if (spark_version(sc) < "2.0.0")
    skip("feature importances not available prior to spark 2.0")

  iris_tbl <- testthat_tbl("iris")
  rf <- iris_tbl %>%
    ml_random_forest(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression",
                     sample.rate = 1, col.sample.rate = 1,
                     num.trees = 1L)
  dt <- iris_tbl %>%
    ml_decision_tree(Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
                     type = "regression")

  rf_importance <- ml_tree_feature_importance(rf)
  dt_importance <- ml_tree_feature_importance(dt)
  expect_equal(colnames(rf_importance), c("feature", "importance"))
  expect_equal(nrow(rf_importance), 3)
  expect_equal(rf_importance, dt_importance)
})

test_that("input_cols print correctly", {
  expect_output_file(
    print(ft_vector_assembler(sc, c("foo", "bar"), "features", uid = "va")),
    output_file("print/vector-assembler.txt")
  )
})

test_that("ml_ helper functions print calls correctly (#1393)", {
  car_db <- testthat_tbl("mtcars")

  res1 <- sparklyr::ml_random_forest(x = car_db, formula = mpg ~ ., type = "regression",
                                    feature_subset_strategy = "3", seed = sample.int(10^5, 1))
  call1 <- gsub("^sparklyr::", "", res1$.call)

  res2 <- ml_random_forest(x = car_db, formula = mpg ~ ., type = "regression",
                           feature_subset_strategy = "3", seed = sample.int(10^5, 1))
  call2 <- res2$.call
  expect_identical(call1, call2)
})
