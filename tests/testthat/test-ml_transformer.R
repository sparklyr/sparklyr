skip_connection("ml_transformer")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
sc <- testthat_spark_connection()

test_that("one can program with ft_ functions (.tbl_spark)", {
  iris_tbl <- testthat_tbl("iris")
  foo1 <- function(such_wow) {
    ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = such_wow)
  }
  foo2 <- function(many_cool) {
    foo1(many_cool)
  }
  foo3 <- function(so_quo) {
    foo2(so_quo)
  }

  df <- ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = 0.3) %>%
    collect()
  df1 <- collect(foo1(0.3))
  df2 <- collect(foo2(0.3))
  df3 <- collect(foo3(0.3))

  expect_identical(df, df1)
  expect_identical(df, df2)
  expect_identical(df, df3)
})

test_that("one can program with ft_ function (.spark_connection)", {
  foo1 <- function(such_tidy, such_wow) {
    ft_binarizer(such_tidy, "Petal_Width", "is_big", threshold = such_wow)
  }
  foo2 <- function(many_bangs, many_cool) {
    foo1(many_bangs, many_cool)
  }
  foo3 <- function(so_unquo, so_quo) {
    foo2(so_unquo, so_quo)
  }

  bin1 <- foo1(sc, 0.3)
  bin2 <- foo2(sc, 0.3)
  bin3 <- foo3(sc, 0.3)

  for (binarizer in list(bin1, bin2, bin3)) {
    expect_equivalent(
      ml_param_map(binarizer) %>%
        as.environment(),
      list(
        output_col = "is_big",
        threshold = 0.3,
        input_col = "Petal_Width"
      ) %>%
        as.environment()
    )
  }
})

test_that("print methods for transformers and estimators produce output", {
  transformer <- ft_binarizer(sc, "x", "y", threshold = 0.5)
  expect_output(print(transformer))
  estimator <- ml_logistic_regression(sc)
  expect_output(print(estimator))
})

test_that("ml_predict dispatches across model families", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  mtcars_tbl <- testthat_tbl("mtcars")

  # classification (probabilistic) -> probability_ columns appended
  clf <- ml_logistic_regression(iris_tbl, Species ~ Sepal_Length + Sepal_Width)
  expect_s3_class(clf$model, "ml_classification_model")
  p_clf <- ml_predict(clf, iris_tbl)
  expect_true(any(grepl("^probability_", colnames(p_clf))))

  # classification (non-probabilistic) -> predictions returned as-is
  iris_bin <- iris_tbl %>%
    mutate(is_versicolor = ifelse(Species == "versicolor", "v", "o")) %>%
    select(-Species)
  svc <- ml_linear_svc(iris_bin, is_versicolor ~ .)
  expect_true(inherits(ml_predict(svc), "tbl_spark"))

  # regression, using the model's own dataset when none is supplied
  reg <- ml_linear_regression(mtcars_tbl, mpg ~ wt)
  expect_true("prediction" %in% colnames(ml_predict(reg)))

  # clustering
  km <- ml_kmeans(iris_tbl, ~ Sepal_Length + Sepal_Width, k = 2)
  expect_true(inherits(ml_predict(km), "tbl_spark"))
})

test_that("ml_predict on a recommendation model", {
  ratings <- copy_to(
    sc,
    data.frame(
      user = c(0L, 0L, 1L, 1L),
      item = c(0L, 1L, 0L, 1L),
      rating = c(4, 2, 3, 5)
    ),
    name = random_string("ratings_"),
    overwrite = TRUE
  )
  # the formula interface yields an ml_model_recommendation (vs. a bare
  # transformer), which routes ml_predict() through the recommendation method
  als <- ml_als(ratings, rating ~ user + item, rank = 2, max_iter = 2)
  expect_s3_class(als, "ml_model_recommendation")
  expect_true(inherits(ml_predict(als), "tbl_spark"))
})

test_that("ml_fit / ml_transform / ml_fit_and_transform on an estimator", {
  iris_tbl <- testthat_tbl("iris")
  prepped <- iris_tbl %>%
    ft_vector_assembler(c("Sepal_Length", "Sepal_Width"), "features")
  est <- ml_kmeans(sc, k = 2, features_col = "features")

  model <- ml_fit(est, prepped)
  expect_true(is_ml_transformer(model))

  expect_true(inherits(ml_transform(model, prepped), "tbl_spark"))
  expect_true(inherits(ml_transform(list(model), prepped), "tbl_spark"))
  expect_true(inherits(ml_fit_and_transform(est, prepped), "tbl_spark"))

  # ml_predict.default on a plain transformer routes through ml_transform
  expect_true(inherits(ml_predict(model, prepped), "tbl_spark"))
})

test_that("transform/fit methods reject the wrong object types", {
  iris_tbl <- testthat_tbl("iris")
  transformer <- ft_binarizer(sc, "x", "y")

  expect_error(ml_fit(transformer, iris_tbl), "only applicable to 'ml_estimator'")
  expect_error(
    ml_fit_and_transform(transformer, iris_tbl),
    "only applicable to 'ml_estimator'"
  )
  expect_error(ml_transform("nope", iris_tbl), "must be 'ml_transformer'")
  expect_error(ml_transform(list("nope"), iris_tbl), "must be 'ml_transformer'")
})

test_that("deprecated sdf_ transform/fit/predict wrappers delegate", {
  iris_tbl <- testthat_tbl("iris")
  mtcars_tbl <- testthat_tbl("mtcars")
  prepped <- iris_tbl %>%
    ft_vector_assembler(c("Sepal_Length", "Sepal_Width"), "features")
  est <- ml_kmeans(sc, k = 2, features_col = "features")

  expect_warning(model <- sdf_fit(prepped, est), "deprecated")
  expect_warning(sdf_transform(prepped, model), "deprecated")
  expect_warning(sdf_fit_and_transform(prepped, est), "deprecated")

  reg <- ml_linear_regression(mtcars_tbl, mpg ~ wt)
  expect_warning(sdf_predict(mtcars_tbl, reg), "deprecated")
})

test_clear_cache()
