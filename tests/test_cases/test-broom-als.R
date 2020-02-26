context("broom-als")

test_that("als.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  movies <- data.frame(user   = c(1, 2, 0, 1, 2, 0),
                       item   = c(1, 1, 1, 2, 2, 0),
                       rating = c(3, 1, 2, 4, 5, 4))

  movies_tbl <- sdf_copy_to(sc,
                            movies,
                            name = "movies_tbl",
                            overwrite = TRUE)

  td1 <- movies_tbl %>%
    ml_als(rating ~ user + item) %>%
    tidy() %>%
    dplyr::collect()

  check_tidy(td1, exp.row = 3, exp.col = 3,
             exp.names = c("id", "user_factors", "item_factors"))
})

test_that("als.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()

  movies <- data.frame(user   = c(1, 2, 0, 1, 2, 0),
                       item   = c(1, 1, 1, 2, 2, 0),
                       rating = c(3, 1, 2, 4, 5, 4))

  movies_tbl <- sdf_copy_to(sc,
                            movies,
                            name = "movies_tbl",
                            overwrite = TRUE)

  au1 <- movies_tbl %>%
    ml_als(rating ~ user + item) %>%
    augment() %>%
    dplyr::collect()

  check_tidy(au1, exp.col = 4,
             exp.name = c("user", "item", "rating", ".prediction"))
})

test_that("als.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()

  movies <- data.frame(user   = c(1, 2, 0, 1, 2, 0),
                       item   = c(1, 1, 1, 2, 2, 0),
                       rating = c(3, 1, 2, 4, 5, 4))

  movies_tbl <- sdf_copy_to(sc,
                            movies,
                            name = "movies_tbl",
                            overwrite = TRUE)

  gl1 <- movies_tbl %>%
    ml_als(rating ~ user + item) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("rank", "cold_start_strategy"))
})
