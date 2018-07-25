context("ml recommendation - als")

test_that("ml_als() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_als)
})

test_that("ml_als() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    rating_col = "awef",
    user_col = "qqqq",
    item_col = "ffffffff",
    rank = 11,
    reg_param = 0.2,
    implicit_prefs = TRUE,
    alpha = 2,
    nonnegative = TRUE,
    max_iter = 20,
    num_user_blocks = 15,
    num_item_blocks = 16,
    checkpoint_interval = 12,
    cold_start_strategy = "drop",
    intermediate_storage_level = "MEMORY_ONLY",
    final_storage_level = "MEMORY_ONLY_SER"
  )
  test_param_setting(sc, ml_als, test_args)
})

test_that("ml_recommend() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.2.0")

  user <- c(0, 0, 1, 1, 2, 2)
  item <- c(0, 1, 1, 2, 1, 2)
  rating <- c(4.0, 2.0, 3.0, 4.0, 1.0, 5.0)

  df <- data.frame(user = user, item = item, rating = rating)
  movie_ratings <- sdf_copy_to(sc, df, "movie_rating", overwrite = TRUE)

  als_model <- ml_als(movie_ratings)
  expect_identical(
    als_model %>%
      ml_recommend("users", 2) %>%
      colnames(),
    c("item", "recommendations", "user", "rating")
  )
  expect_identical(
    als_model %>%
      ml_recommend("items", 2) %>%
      colnames(),
    c("user", "recommendations", "item", "rating")
  )
})
