context("broom-lda")

test_that("lda.tidy() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0")

  samples <-  data.frame(text = c("The cat sat on the mat.",
                                  "The dog ate my homework."))

  lines_tbl <- sdf_copy_to(sc,
                         samples,
                         name = "lines_tbl",
                         overwrite = TRUE)

  td1 <- lines_tbl %>%
    ml_lda(~text, k = 3) %>%
    tidy()

  check_tidy(td1, exp.row = 3, exp.col = 4,
             exp.names = c("topic", "termIndices",
                           "termWeights", "doc_concentrarion"))

  expect_equal(td1$doc_concentrarion,
               c(0.3269, 0.3392, 0.3271),
               tolerance = 0.001)
})

test_that("lda.augment() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()

  samples <-  data.frame(text = c("The cat sat on the mat.",
                                  "The dog ate my homework."))

  lines_tbl <- sdf_copy_to(sc,
                           samples,
                           name = "lines_tbl",
                           overwrite = TRUE)

  au1 <- lines_tbl %>%
    ml_lda(~text, k = 3) %>%
    augment()

  check_tidy(au1, exp.row = 9, exp.col = 4,
             exp.name = c("vocabulary", "topic_0",
                          "topic_1", "topic_2"))
})

test_that("lda.glance() works", {
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()

  samples <-  data.frame(text = c("The cat sat on the mat.",
                                  "The dog ate my homework."))

  lines_tbl <- sdf_copy_to(sc,
                           samples,
                           name = "lines_tbl",
                           overwrite = TRUE)

  gl1 <- lines_tbl %>%
    ml_lda(~text, k = 3) %>%
    glance()

  check_tidy(gl1, exp.row = 1,
             exp.names = c("k", "vocab_size",
                           "learning_decay", "optimizer"))
})
