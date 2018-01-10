context("ml persistence")

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

test_that("ml_save/ml_load work for unnested pipelines", {
  p1 <- ml_pipeline(sc) %>%
    ft_tokenizer("x", "y") %>%
    ft_binarizer("in", "out", 0.5)
  path <- tempfile()
  ml_save(p1, path)
  p2 <- ml_load(sc, path)


  p1_params <- p1 %>%
    ml_stages() %>%
    lapply(ml_param_map)
  p2_params <- p2 %>%
    ml_stages() %>%
    lapply(ml_param_map)

  expect_equal(p1$uid, p2$uid)
  expect_equal(p1_params, p2_params)
})

test_that("ml_save/ml_load work for nested pipeline", {
  p1a <- ml_pipeline(ft_tokenizer(sc, "x", "y"))
  p1b <- ft_binarizer(sc, "in", "out", 0.5)
  p1 <- ml_pipeline(p1a, p1b)
  path <- tempfile()
  ml_save(p1, path)
  p2 <- ml_load(sc, path)

  p1_tok_params <- p1 %>%
    ml_stage(1) %>%
    ml_stage(1) %>%
    ml_param_map()
  p2_tok_params <- p2 %>%
    ml_stage(1) %>%
    ml_stage(1) %>%
    ml_param_map()
  p1_bin_params <- p1 %>%
    ml_stage(2) %>%
    ml_param_map()
  p2_bin_params <- p2 %>%
    ml_stage(2) %>%
    ml_param_map()

  expect_equal(p1$uid, p2$uid)
  expect_equal(p1_tok_params, p2_tok_params)
  expect_equal(p1_bin_params, p2_bin_params)
  expect_equal(p1$stage_uids, p2$stage_uids)
})

test_that("ml_fit() returns a ml_pipeline_model", {

  tokenizer <- ft_tokenizer(sc, input_col = "text", output_col = "words")
  hashing_tf <- ft_hashing_tf(sc, input_col = "words", output_col = "features")
  lr <- ml_logistic_regression(sc, max_iter = 10, lambda = 0.001)
  pipeline <- ml_pipeline(tokenizer, hashing_tf, lr)

  model <- ml_fit(pipeline, training_tbl)
  expect_equal(class(model)[1], "ml_pipeline_model")
})

test_that("ml_[save/load]_model() work for ml_pipeline_model", {
  pipeline <- ml_pipeline(sc) %>%
    ft_tokenizer("text", "words") %>%
    ft_hashing_tf("words", "features") %>%
    ml_logistic_regression(max_iter = 10, lambda = 0.001)
  model1 <- ml_fit(pipeline, training_tbl)
  path <- tempfile("model")
  ml_save(model1, path)
  model2 <- ml_load(sc, path)
  expect_equal(model1$stage_uids, model2$stage_uids)

  score_test_set <- function(x, data) {
    spark_jobj(x) %>%
      invoke("transform", spark_dataframe(data)) %>%
      sdf_register() %>%
      pull(probability)
  }
  expect_equal(score_test_set(model1, test_tbl), score_test_set(model2, test_tbl))
})

test_that("we can ml_save/load a feature transformer", {
  bin1 <- ft_binarizer(sc, "in", "out", threshold = 0.6, uid = "bin1")
  path <- tempfile("ftbin")
  ml_save(bin1, path)
  bin2 <- ml_load(sc, path)
  expect_equal(ml_param_map(bin1), ml_param_map(bin2))
})

test_that("we can save a ml_model and load a pipeline model back", {
  test_requires_version("2.0.0", "RFormula export requires Spark 2.0+")
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))

  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  m1 <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width"
  )

  path <- tempfile("lr_mlmodel")
  ml_save(m1, path)
  m2 <- ml_load(sc, path)

  expect_equal(ml_stage(m1$pipeline_model, 1) %>%
                 ml_param_map(),
               ml_stage(m2, 1) %>%
                 ml_param_map())

  expect_equal(ml_stage(m1$pipeline_model, 2) %>%
                 ml_param_map(),
               ml_stage(m2, 2) %>%
                 ml_param_map())

  expect_identical(ml_stage(m1$pipeline_model, 2)$coefficients,
               ml_stage(m2, 2)$coefficients
  )
})

test_that("we can fit a pipeline saved then loaded from ml_model", {
  test_requires_version("2.0.0", "RFormula export requires Spark 2.0+")
  set.seed(42)
  iris_weighted <- iris %>%
    dplyr::mutate(weights = rpois(nrow(iris), 1) + 1,
                  ones = rep(1, nrow(iris)),
                  versicolor = ifelse(Species == "versicolor", 1L, 0L))

  iris_weighted_tbl <- testthat_tbl("iris_weighted")

  m1 <- ml_logistic_regression(
    iris_weighted_tbl,
    formula = "versicolor ~ Sepal_Width + Petal_Length + Petal_Width"
  )

  path <- tempfile("lr_mlmodel")
  ml_save(m1, path, overwrite = TRUE, type = "pipeline")
  pipeline <- ml_load(sc, path)
  m2 <- pipeline %>%
    ml_fit(iris_weighted_tbl)
  expect_identical(ml_stage(m1$pipeline_model, 2)$coefficients,
                   ml_stage(m2, 2)$coefficients
  )
})
