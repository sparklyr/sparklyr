skip_connection("ml_pipeline")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

sc <- testthat_spark_connection()

training <- dplyr::tibble(
  id = 0:3L,
  text = c(
    "a b c d e spark",
    "b d",
    "spark f g h",
    "hadoop mapreduce"
  ),
  label = c(1, 0, 1, 0)
)

training_tbl <- testthat_tbl("training")

test <- dplyr::tibble(
  id = 4:7L,
  text = c("spark i j k", "l m n", "spark hadoop spark", "apache hadoop")
)
test_tbl <- testthat_tbl("test")

test_that("ml_pipeline() returns a c('ml_pipeline', 'ml_estimator', 'ml_pipeline_stage')", {
  p <- ml_pipeline(sc)
  expect_equal(class(p), c("ml_pipeline", "ml_estimator", "ml_pipeline_stage"))
  expect_equal(ml_stages(p), NULL)
  expect_equal(jobj_class(spark_jobj(p))[1], "Pipeline")
  uid_prefix <- gsub(pattern = "_.+$", replacement = "", p$uid)
  expect_equal(uid_prefix, "pipeline")
})

test_that("ml_pipeline() combines pipeline_stages into a pipeline", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)
  pipeline <- ml_pipeline(tokenizer, binarizer)
  individual_stage_uids <- c(tokenizer$uid, binarizer$uid)
  expect_equal(pipeline$stage_uids, individual_stage_uids)
  expect_equal(
    class(pipeline),
    c("ml_pipeline", "ml_estimator", "ml_pipeline_stage")
  )
})

test_that("we can create nested pipelines", {
  p0 <- ml_pipeline(sc)
  tokenizer <- ft_tokenizer(sc, "x", "y")
  pipeline <- ml_pipeline(p0, tokenizer)
  expect_equal(class(ml_stage(pipeline, 1))[1], "ml_pipeline")
  expect_equal(ml_stage(pipeline, 1) %>% ml_stages(), NULL)
})

test_that("ml_transformer.ml_pipeline() works as expected", {
  tokenizer <- ft_tokenizer(sc, "x", "y")
  binarizer <- ft_binarizer(sc, "in", "out", 0.5)

  p1 <- ml_pipeline(tokenizer, binarizer)

  p2 <- ml_pipeline(sc) %>%
    ft_tokenizer("x", "y") %>%
    ft_binarizer("in", "out", 0.5)

  p1_params <- p1 %>%
    ml_stages() %>%
    lapply(ml_param_map) %>%
    lapply(as.environment)
  p2_params <- p2 %>%
    ml_stages() %>%
    lapply(ml_param_map) %>%
    lapply(as.environment)
  expect_equal(p1_params, p2_params)
  expect_equal(class(p2)[1], "ml_pipeline")
})

test_that("empty pipeline has no stages", {
  expect_null(ml_pipeline(sc) %>% ml_stages())
})

test_that("pipeline printing works", {
  output <- capture.output(ml_pipeline(sc))
  expect_identical(output[1], "Pipeline (Estimator) with no stages")

  output <- capture.output(ml_pipeline(ft_binarizer(sc, "in", "out")))
  expect_identical(output[1], "Pipeline (Estimator) with 1 stage")

  output <- capture.output(ml_pipeline(
    ft_binarizer(sc, "in", "out"),
    ml_logistic_regression(sc)
  ))
  expect_identical(output[1], "Pipeline (Estimator) with 2 stages")
  expect_identical(output[4], "  |--1 Binarizer (Transformer)")
  expect_identical(output[6], "  |     (Parameters -- Column Names)")
  expect_identical(output[9], "  |--2 LogisticRegression (Estimator)")
})

test_that("Error when specifying formula without tbl_spark for ml_ routines", {
  expect_error(
    ml_pipeline(sc) %>%
      ml_logistic_regression(Species ~ Petal_Length),
    "`formula` may only be specified when `x` is a `tbl_spark`\\."
  )

  expect_error(
    ml_logistic_regression(sc, Species ~ Petal_Length),
    "`formula` may only be specified when `x` is a `tbl_spark`\\."
  )
})

test_that("ml_transform() fails on estimators", {
  iris_tbl <- testthat_tbl("iris")
  string_indexer <- ft_string_indexer(sc, "Species", "species_idx")
  expect_error(
    string_indexer %>%
      ml_transform(iris_tbl),
    "Transformers must be 'ml_transformer' objects"
  )
})

test_that("ml_fit() and ml_fit_and_transform() fail on transformers", {
  iris_tbl <- testthat_tbl("iris")
  binarizer <- ft_binarizer(sc, "Petal_Width", "petal_width_binarized")
  expect_error(
    binarizer %>%
      ml_fit(iris_tbl),
    "is only applicable to"
  )
  expect_error(
    binarizer %>%
      ml_fit_and_transform(iris_tbl),
    "is only applicable to"
  )
})

test_that("ml_stage() and ml_stages() work properly", {
  pipeline <- ml_pipeline(sc) %>%
    ft_tokenizer("a", "b", uid = "tok1") %>%
    ft_tokenizer("c", "d", uid = "tok2") %>%
    ft_binarizer("e", "f", uid = "bin1")

  expect_error(
    ml_stage(pipeline, "blah"),
    "stage not found"
  )
  expect_error(
    ml_stage(pipeline, "tok"),
    "multiple stages found"
  )
  expect_equal(
    pipeline %>%
      ml_stage("bin") %>%
      ml_uid(),
    "bin1"
  )
  expect_error(
    ml_stages(pipeline, c("blah")),
    "no stages found for identifier blah"
  )
  expect_error(
    ml_stages(pipeline, c("tok", "bin")),
    "multiple stages found for identifier tok"
  )
  expect_equal(
    ml_stages(pipeline, c("tok1", "bin")) %>%
      sapply(ml_uid),
    c("tok1", "bin1")
  )

  expect_equal(
    ml_stages(pipeline) %>%
      sapply(ml_uid),
    c("tok1", "tok2", "bin1")
  )

  expect_equal(
    ml_stage(pipeline, 1) %>%
      ml_uid(),
    "tok1"
  )
  expect_equal(
    ml_stages(pipeline, 1) %>%
      sapply(ml_uid),
    "tok1"
  )
  expect_equal(
    ml_stages(pipeline, 1:2) %>%
      sapply(ml_uid),
    c("tok1", "tok2")
  )
})

test_that("ml_is_set works", {
  lr <- ml_logistic_regression(sc, reg_param = 0L)
  expect_true(ml_is_set(lr, "reg_param"))
  expect_false(ml_is_set(lr, "thresholds"))

  expect_true(ml_is_set(spark_jobj(lr), "reg_param"))
  expect_false(ml_is_set(spark_jobj(lr), "thresholds"))
})

test_that("ml_transform take list of transformers (#1444)", {
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")
  string_indexer <- ft_string_indexer(sc, "Species", "label") %>%
    ml_fit(iris_tbl)
  pipeline <- ml_pipeline(string_indexer) %>%
    ft_vector_assembler(c("Petal_Width", "Petal_Length"), "features") %>%
    ml_logistic_regression() %>%
    ft_index_to_string(
      "prediction",
      "predicted_label",
      labels = ml_labels(string_indexer)
    )
  pipeline_model <- ml_fit(pipeline, iris_tbl)
  stages <- pipeline_model %>%
    ml_stages(c("vector_assembler", "logistic", "index_to_string"))

  transformed1 <- ml_transform(stages, iris_tbl) %>%
    dplyr::pull(prediction)
  transformed2 <- Reduce(
    function(transformer, data) ml_transform(data, transformer),
    stages,
    init = iris_tbl
  ) %>%
    dplyr::pull(prediction)
  expect_equal(transformed1, transformed2)
})

test_clear_cache()
