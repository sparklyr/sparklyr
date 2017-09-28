context("ml pipeline")

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

test_that("ml_pipeline() returns a c('ml_pipeline', 'ml_estimator', 'ml_pipeline_stage')", {
  p <- ml_pipeline(sc)
  expect_equal(class(p), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
  expect_equal(p$stages, NA)
  expect_equal(p$type, "org.apache.spark.ml.Pipeline")
  uid_prefix <- gsub(pattern = "_.+$", replacement = "", p$uid)
  expect_equal(uid_prefix, "pipeline")
})

test_that("ml_pipeline() combines pipeline_stages into a pipeline", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)
  pipeline <- ml_pipeline(tokenizer, binarizer)
  individual_stage_uids <- c(tokenizer$uid, binarizer$uid)
  expect_equal(pipeline$stage_uids, individual_stage_uids)
  expect_equal(class(pipeline), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
})

test_that("we can create nested pipelines", {
  p0 <- ml_pipeline(sc)
  tokenizer <- ft_tokenizer(sc, "x", "y")
  pipeline <- ml_pipeline(p0, tokenizer)
  expect_equal(class(pipeline$stages[[1]])[1], "ml_pipeline")
  expect_equal(pipeline$stages[[1]]$stages, NA)
})

test_that("ml_transformer.ml_pipeline() works as expected", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)

  p1 <- ml_pipeline(tokenizer, binarizer)

  p2 <- ml_pipeline(sc) %>%
    ft_tokenizer("x", "y") %>%
    ft_binarizer("in", "out", 0.5)

  p1_params <- p1$stages %>%
    lapply(function(x) x$param_map)
  p2_params <- p2$stages %>%
    lapply(function(x) x$param_map)
  expect_equal(p1_params, p2_params)
  expect_equal(class(p2)[1], "ml_pipeline")
})

test_that("ml_transform() fails on estimators", {
  iris_tbl <- testthat_tbl("iris")
  string_indexer <- ft_string_indexer(sc, "Species", "species_idx")
  expect_error(string_indexer %>%
                 ml_transform(iris_tbl),
               "cannot invoke 'transform' on estimators")
})

test_that("ml_fit() and ml_fit_and_transform() fail on transformers", {
  iris_tbl <- testthat_tbl("iris")
  binarizer <- ft_binarizer(sc, "Petal_Width", "petal_width_binarized")
  expect_error(binarizer %>%
                 ml_fit(iris_tbl),
               "cannot invoke 'fit' on transformers")
  expect_error(binarizer %>%
                 ml_fit_and_transform(iris_tbl),
               "cannot invoke 'fit' on transformers")
})

test_that("ml_stage() and ml_stages() work properly", {
  pipeline <- ml_pipeline(sc) %>%
    ft_tokenizer("a", "b", uid = "tok1") %>%
    ft_tokenizer("c", "d", uid = "tok2") %>%
    ft_binarizer("e", "f", uid = "bin1")

  expect_error(ml_stage(pipeline, "blah"),
               "stage not found")
  expect_error(ml_stage(pipeline, "tok"),
               "multiple stages found")
  expect_equal(pipeline %>%
                 ml_stage("bin") %>%
                 ml_uid(),
               "bin1")
  expect_error(ml_stages(pipeline, c("blah")),
               "no stages found for identifier blah")
  expect_error(ml_stages(pipeline, c("tok", "bin")),
               "multiple stages found for identifier tok")
  expect_equal(ml_stages(pipeline, c("tok1", "bin")) %>%
                 sapply(ml_uid),
               c("tok1", "bin1"))


})
