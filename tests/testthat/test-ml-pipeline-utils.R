context("ml pipeline utils")

sc <- testthat_spark_connection()

test_that("ml_transform() fails on estimators", {
  iris_tbl <- testthat_tbl("iris")
  string_indexer <- ft_string_indexer(sc, "Species", "species_idx")
  expect_error(string_indexer %>%
                 ml_transform(iris_tbl),
               "Transformers must be 'ml_transformer' objects")
})

test_that("ml_fit() and ml_fit_and_transform() fail on transformers", {
  iris_tbl <- testthat_tbl("iris")
  binarizer <- ft_binarizer(sc, "Petal_Width", "petal_width_binarized")
  expect_error(binarizer %>%
                 ml_fit(iris_tbl),
               "is only applicable to")
  expect_error(binarizer %>%
                 ml_fit_and_transform(iris_tbl),
               "is only applicable to")
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

  expect_equal(ml_stages(pipeline) %>%
                 sapply(ml_uid),
               c("tok1", "tok2", "bin1"))

  expect_equal(ml_stage(pipeline, 1) %>%
                 ml_uid(),
               "tok1")
  expect_equal(ml_stages(pipeline, 1) %>%
                 sapply(ml_uid),
               "tok1")
  expect_equal(ml_stages(pipeline, 1:2) %>%
                 sapply(ml_uid),
               c("tok1", "tok2"))
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
  string_indexer <- ft_string_indexer(sc, "Species", "label", dataset = iris_tbl)
  pipeline <- ml_pipeline(string_indexer) %>%
    ft_vector_assembler(c("Petal_Width", "Petal_Length"), "features") %>%
    ml_logistic_regression() %>%
    ft_index_to_string("prediction", "predicted_label",
                       labels = ml_labels(string_indexer))
  pipeline_model <- ml_fit(pipeline, iris_tbl)
  stages <- pipeline_model %>%
    ml_stages(c("vector_assembler", "logistic", "index_to_string"))

  transformed1 <- ml_transform(stages, iris_tbl) %>%
    dplyr::pull(prediction)
  transformed2 <- Reduce(sdf_transform, stages, init = iris_tbl) %>%
    dplyr::pull(prediction)
  expect_equal(transformed1, transformed2)
})
