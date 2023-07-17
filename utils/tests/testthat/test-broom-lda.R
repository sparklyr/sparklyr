skip_connection("broom-lda")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("lda.tidy() works", {

  ## ---------------- Connection and data upload to Spark ----------------------

  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  samples <- data.frame(text = c(
    "The cat sat on the mat.",
    "The dog ate my homework."
  ))

  lines_tbl <- sdf_copy_to(sc,
    samples,
    name = "lines_tbl",
    overwrite = TRUE
  )

  lda_model <- lines_tbl %>%
    ml_lda(~text, k = 3)

  ## ----------------------------- tidy() --------------------------------------

  td1 <- tidy(lda_model)

  check_tidy(td1,
    exp.row = 18, exp.col = 3,
    exp.names = c("topic", "term", "beta")
  )

  # account for LDA method behavior change in Spark 3.0.0
  expect_equal(td1$beta[1:3],
    ifelse(spark_version(sc) < "3.0.0",
      list(c(0.8773, 0.9466, 1.2075)),
      list(c(0.8790, 0.9478, 1.1515))
    )[[1]],
    tolerance = 0.001,
    scale = 1
  )

  ## --------------------------- augment() -------------------------------------

  expect_warning_on_arrow(
    au1 <- lda_model %>%
      augment() %>%
      collect()
  )

  check_tidy(au1,
    exp.col = 2,
    exp.name = c("text", ".topic")
  )

  ## ---------------------------- glance() -------------------------------------

  gl1 <- glance(lda_model)

  check_tidy(gl1,
    exp.row = 1,
    exp.names = c(
      "k", "vocab_size",
      "learning_decay", "optimizer"
    )
  )
})
