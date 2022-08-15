skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("als tidiers works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  movies <- data.frame(
    user = c(1, 2, 0, 1, 2, 0),
    item = c(1, 1, 1, 2, 2, 0),
    rating = c(3, 1, 2, 4, 5, 4)
  )

  movies_tbl <- sdf_copy_to(sc,
    movies,
    name = "movies_tbl",
    overwrite = TRUE
  )

  ## ----------------------------- tidy() --------------------------------------

  als_model <- movies_tbl %>%
    ml_als(rating ~ user + item)

  td1 <-  tidy(als_model) %>%
    collect()

  check_tidy(td1,
    exp.row = 3, exp.col = 3,
    exp.names = c("id", "user_factors", "item_factors")
  )

  ## --------------------------- augment() -------------------------------------

  au1 <- als_model %>%
    augment() %>%
    collect()

  check_tidy(au1,
    exp.col = 4,
    exp.name = c("user", "item", "rating", ".prediction")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(als_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c("rank", "cold_start_strategy")
  )
})
