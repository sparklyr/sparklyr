skip_connection("ml_model_constructors")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("supervised regression model: construct, print, spark_jobj", {
  test_requires_version("3.0.0")
  m <- testthat_tbl("iris") %>%
    ml_gbt_regressor(Sepal_Length ~ Petal_Length + Petal_Width)
  expect_s3_class(m, "ml_model_regression")
  expect_s3_class(spark_jobj(m), "spark_jobj")
  expect_match(paste(capture.output(print(m)), collapse = " "), "Formula")
})

test_that("classification model is constructed (with label metadata)", {
  test_requires_version("3.0.0")
  bin <- testthat_tbl("iris") %>%
    dplyr::mutate(lab = ifelse(Species == "setosa", "y", "n"))
  m <- bin %>% ml_gbt_classifier(lab ~ Petal_Length + Petal_Width)
  expect_s3_class(m, "ml_model_classification")
})

test_that("clustering model is constructed", {
  test_requires_version("3.0.0")
  m <- ml_kmeans(
    testthat_tbl("iris"),
    Species ~ Petal_Length + Petal_Width,
    k = 2
  )
  expect_s3_class(m, "ml_model_clustering")
})

test_that("recommendation model (ALS) is constructed", {
  test_requires_version("3.0.0")
  ratings <- sdf_copy_to(
    sc,
    data.frame(
      user = c(0L, 0L, 1L, 1L),
      item = c(0L, 1L, 0L, 1L),
      rating = c(4, 2, 3, 5)
    ),
    "mmc_rat",
    overwrite = TRUE
  )
  m <- ml_als(ratings, rating ~ user + item)
  expect_s3_class(m, "ml_model_recommendation")
})

test_clear_cache()
