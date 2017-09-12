context("ml classification")

sc <- testthat_spark_connection()

training <- data_frame(
  id = 0:3L,
  text = c("a b c d e spark",
           "b d",
           "spark f g h",
           "hadoop mapreduce"),
  label = c(1, 0, 1, 0)
)

training_tbl <- testthat_tbl("training")

test <- data_frame(
  id = 4:7L,
  text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
)
test_tbl <- testthat_tbl("test")

# test_that("ml_logistic_regression() returns params", {
#   lr <- ml_logistic_regression(sc, intercept = TRUE, elastic_net_param = 0)
#   expected_params <- list(intercept = TRUE, elastic_net_param = 0)
#   params <- lr$param_map
#   expect_equal(setdiff(expected_params, params), list())
# })
#
# test_that("ml_logistic_regression() does input checking", {
#   expect_error(ml_logistic_regression(sc, elastic_net_param = "foo"),
#                "length-one numeric vector")
#   expect_equal(ml_logistic_regression(sc, max_iter = 25)$param_map$max_iter,
#                25L)
# })

test_that("ml_logistic_regression.tbl_spark() works properly", {
  training_tbl <- testthat_tbl("training")
  test_tbl <- testthat_tbl("test")

  pipeline <- ml_pipeline(sc) %>%
    ml_tokenizer("text", "words") %>%
    ml_hashing_tf("words", "features", num_features = 1000) %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001)

  m_1 <- pipeline %>%
    ml_fit(training_tbl) %>%
    ml_transform(test_tbl) %>%
    dplyr::pull(probability)

  m_2 <- training_tbl %>%
    ml_tokenizer("text", "words") %>%
    ml_hashing_tf("words", "features", num_features = 1000) %>%
    ml_logistic_regression(max_iter = 10, reg_param = 0.001) %>%
    ml_transform(test_tbl %>%
                   ml_tokenizer("text", "words") %>%
                   ml_hashing_tf("words", "features", num_features = 1000)) %>%
    dplyr::pull(probability)

  expect_equal(m_1, m_2)

})
