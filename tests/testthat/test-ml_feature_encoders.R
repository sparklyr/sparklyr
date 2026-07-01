# ---------------------------------------------------------------------------
# Connection-free contract tests for the version-gated OneHotEncoder code.
# These exercise branches CI can no longer run: the Spark < 3.0 single-column
# OneHotEncoder path and the Spark 2.3-2.4 OneHotEncoderEstimator family. Rather
# than a live cluster, we mock the JVM-facing layer (spark_pipeline_stage /
# invoke / new_ml_*) and assert the exact sequence of calls the R code emits,
# which pins the interop contract for this otherwise-frozen 2.x code. Each mock
# force()s its piped input so the whole magrittr chain evaluates inner-to-outer.
# ---------------------------------------------------------------------------
capture_ml_calls <- function(expr, required = FALSE) {
  calls <- list()
  record <- function(entry) {
    calls[[length(calls) + 1]] <<- entry
    "<jobj>"
  }
  with_mocked_bindings(
    spark_require_version = function(...) invisible(NULL),
    is_required_spark = function(x, required_version) required,
    spark_pipeline_stage = function(sc, class, ...) {
      record(c(list(call = "spark_pipeline_stage", class = class), list(...)))
    },
    invoke = function(jobj, method, ...) {
      force(jobj)
      record(list(call = "invoke", method = method, args = list(...)))
    },
    new_ml_one_hot_encoder = function(jobj) {
      force(jobj)
      record(list(call = "new_ml_one_hot_encoder"))
    },
    new_ml_one_hot_encoder_estimator = function(jobj) {
      force(jobj)
      record(list(call = "new_ml_one_hot_encoder_estimator"))
    },
    .package = "sparklyr",
    force(expr)
  )
  calls
}

ml_enc_fake_sc <- structure(list(), class = "spark_connection")

test_that("ft_one_hot_encoder() (Spark < 3.0) builds a single-column OneHotEncoder", {
  calls <- capture_ml_calls(
    ft_one_hot_encoder(
      ml_enc_fake_sc,
      input_cols = "a",
      output_cols = "x",
      handle_invalid = "keep",
      drop_last = FALSE,
      uid = "ohe_1"
    )
  )
  expect_equal(
    calls,
    list(
      list(
        call = "spark_pipeline_stage",
        class = "org.apache.spark.ml.feature.OneHotEncoder",
        input_col = "a",
        output_col = "x",
        uid = "ohe_1"
      ),
      list(call = "invoke", method = "setDropLast", args = list(FALSE)),
      list(call = "new_ml_one_hot_encoder")
    )
  )
})

test_that("ft_one_hot_encoder() (Spark < 3.0) rejects multiple columns", {
  expect_error(
    capture_ml_calls(
      ft_one_hot_encoder(
        ml_enc_fake_sc,
        input_cols = c("a", "b"),
        output_cols = c("x", "y")
      )
    ),
    "does not support encoding multiple columns"
  )
})

test_that("ft_one_hot_encoder_estimator() builds an OneHotEncoderEstimator", {
  calls <- capture_ml_calls(
    ft_one_hot_encoder_estimator(
      ml_enc_fake_sc,
      input_cols = c("a", "b"),
      output_cols = c("x", "y"),
      handle_invalid = "keep",
      drop_last = FALSE,
      uid = "ohe_est_1"
    )
  )
  expect_equal(
    calls,
    list(
      list(
        call = "spark_pipeline_stage",
        class = "org.apache.spark.ml.feature.OneHotEncoderEstimator",
        input_cols = list("a", "b"),
        output_cols = list("x", "y"),
        uid = "ohe_est_1"
      ),
      list(call = "invoke", method = "setHandleInvalid", args = list("keep")),
      list(call = "invoke", method = "setDropLast", args = list(FALSE)),
      list(call = "new_ml_one_hot_encoder_estimator")
    )
  )
})

test_that("new_ml_one_hot_encoder() wraps as a transformer on Spark < 3.0", {
  with_mocked_bindings(
    is_required_spark = function(x, required_version) FALSE,
    new_ml_transformer = function(jobj, class) list(class = class),
    .package = "sparklyr",
    expect_equal(new_ml_one_hot_encoder("<jobj>")$class, "ml_one_hot_encoder")
  )
})

test_that("new_ml_one_hot_encoder_estimator() wraps as an estimator", {
  with_mocked_bindings(
    new_ml_estimator = function(jobj, class) list(class = class),
    .package = "sparklyr",
    expect_equal(
      new_ml_one_hot_encoder_estimator("<jobj>")$class,
      "ml_one_hot_encoder_estimator"
    )
  )
})

test_that("new_ml_one_hot_encoder_estimator_model() reads category sizes", {
  with_mocked_bindings(
    invoke = function(jobj, method, ...) {
      expect_equal(method, "categorySizes")
      c(2L, 3L)
    },
    new_ml_transformer = function(jobj, category_sizes, class) {
      list(category_sizes = category_sizes, class = class)
    },
    .package = "sparklyr",
    {
      res <- new_ml_one_hot_encoder_estimator_model("<jobj>")
      expect_equal(res$category_sizes, c(2L, 3L))
      expect_equal(res$class, "ml_one_hot_encoder_model")
    }
  )
})

# The .ml_pipeline / .tbl_spark methods are thin dispatchers that forward to the
# .spark_connection method (called by name, so it is mockable) and then add the
# stage / fit / transform. We capture the forwarded args to confirm the
# Spark < 3.0 path drops `handle_invalid`, and that estimator vs transformer
# stages route to fit-and-transform vs transform.
ml_enc_fake_pipeline <- structure(list(), class = "ml_pipeline")
ml_enc_fake_tbl <- structure(list(), class = "tbl_spark")

test_that("ft_one_hot_encoder(ml_pipeline) (Spark < 3.0) drops handle_invalid", {
  forwarded <- NULL
  with_mocked_bindings(
    spark_connection = function(x, ...) ml_enc_fake_sc,
    is_required_spark = function(x, required_version) FALSE,
    ft_one_hot_encoder.spark_connection = function(
      x,
      input_cols,
      output_cols,
      handle_invalid,
      drop_last,
      uid,
      ...
    ) {
      forwarded <<- list(
        input_cols = input_cols,
        handle_invalid_missing = missing(handle_invalid)
      )
      "<stage>"
    },
    ml_add_stage = function(x, stage) list(stage = stage),
    .package = "sparklyr",
    {
      res <- ft_one_hot_encoder(
        ml_enc_fake_pipeline,
        input_cols = "a",
        output_cols = "x",
        uid = "u"
      )
      expect_equal(res$stage, "<stage>")
      expect_equal(forwarded$input_cols, "a")
      expect_true(forwarded$handle_invalid_missing)
    }
  )
})

test_that("ft_one_hot_encoder(tbl_spark) (Spark < 3.0) transforms the data", {
  with_mocked_bindings(
    spark_connection = function(x, ...) ml_enc_fake_sc,
    is_required_spark = function(x, required_version) FALSE,
    ft_one_hot_encoder.spark_connection = function(x, ...) "<transformer>",
    is_ml_transformer = function(x) TRUE,
    ml_transform = function(stage, x) list(transformed = stage),
    .package = "sparklyr",
    {
      res <- ft_one_hot_encoder(
        ml_enc_fake_tbl,
        input_cols = "a",
        output_cols = "x"
      )
      expect_equal(res$transformed, "<transformer>")
    }
  )
})

test_that("ft_one_hot_encoder_estimator(ml_pipeline) adds the stage", {
  forwarded <- NULL
  with_mocked_bindings(
    spark_connection = function(x, ...) ml_enc_fake_sc,
    ft_one_hot_encoder_estimator.spark_connection = function(
      x,
      input_cols,
      ...
    ) {
      forwarded <<- input_cols
      "<stage>"
    },
    ml_add_stage = function(x, stage) list(stage = stage),
    .package = "sparklyr",
    {
      res <- ft_one_hot_encoder_estimator(
        ml_enc_fake_pipeline,
        input_cols = "a",
        output_cols = "x",
        uid = "u"
      )
      expect_equal(res$stage, "<stage>")
      expect_equal(forwarded, "a")
    }
  )
})

test_that("ft_one_hot_encoder_estimator(tbl_spark) fits and transforms", {
  with_mocked_bindings(
    spark_connection = function(x, ...) ml_enc_fake_sc,
    ft_one_hot_encoder_estimator.spark_connection = function(x, ...) {
      "<estimator>"
    },
    is_ml_transformer = function(x) FALSE,
    ml_fit_and_transform = function(stage, x) list(fitted = stage),
    .package = "sparklyr",
    {
      res <- ft_one_hot_encoder_estimator(
        ml_enc_fake_tbl,
        input_cols = "a",
        output_cols = "x"
      )
      expect_equal(res$fitted, "<estimator>")
    }
  )
})

test_that("ft_one_hot_encoder_estimator(tbl_spark) transforms an already-fit stage", {
  with_mocked_bindings(
    spark_connection = function(x, ...) ml_enc_fake_sc,
    ft_one_hot_encoder_estimator.spark_connection = function(x, ...) {
      "<transformer>"
    },
    is_ml_transformer = function(x) TRUE,
    ml_transform = function(stage, x) list(transformed = stage),
    .package = "sparklyr",
    {
      res <- ft_one_hot_encoder_estimator(
        ml_enc_fake_tbl,
        input_cols = "a",
        output_cols = "x"
      )
      expect_equal(res$transformed, "<transformer>")
    }
  )
})

test_that("new_ml_one_hot_encoder_model() reads category sizes (Spark 3.0+ model)", {
  with_mocked_bindings(
    spark_require_version = function(...) invisible(NULL),
    spark_connection = function(x, ...) ml_enc_fake_sc,
    invoke = function(jobj, method, ...) {
      expect_equal(method, "categorySizes")
      c(1L, 4L)
    },
    new_ml_transformer = function(jobj, category_sizes, class) {
      list(category_sizes = category_sizes, class = class)
    },
    .package = "sparklyr",
    {
      res <- new_ml_one_hot_encoder_model("<jobj>")
      expect_equal(res$category_sizes, c(1L, 4L))
      expect_equal(res$class, "ml_one_hot_encoder_model")
    }
  )
})

skip_connection("ml_feature_encoders")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("ft_one_hot_encoder() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_one_hot_encoder)
})

test_that("ft_one_hot_encoder() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "foo1"),
    output_cols = c("bar", "bar1"),
    drop_last = FALSE
  )
  test_param_setting(sc, ft_one_hot_encoder, test_args)
})

test_that("ft_one_hot_encoder() works", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.3.0") {
    expect_equal(
      iris_tbl %>%
        ft_string_indexer("Species", "indexed") %>%
        ft_one_hot_encoder("indexed", "encoded") %>%
        pull(encoded) %>%
        unique(),
      list(c(0, 0), c(1, 0), c(0, 1))
    )
  } else {
    num_cols <- if (spark_version(sc) >= "3.0.0") 2 else 1
    indexed_cols <- paste0("indexed", seq(num_cols))
    encoded_cols <- paste0("encoded", seq(num_cols))

    encoded_tbl <- iris_tbl %>%
      mutate(Species1 = Species, Species2 = Species) %>%
      ft_string_indexer(
        "Species1",
        "indexed1",
        string_order_type = "alphabetDesc"
      ) %>%
      ft_string_indexer(
        "Species2",
        "indexed2",
        string_order_type = "alphabetDesc"
      ) %>%
      ft_one_hot_encoder(indexed_cols, encoded_cols) %>%
      compute()
    for (x in encoded_cols) {
      expect_warning_on_arrow(
        expect_setequal(
          encoded_tbl %>% pull(!!x) %>% unique(),
          list(c(0, 0), c(0, 1), c(1, 0))
        )
      )
    }
  }
})

test_that("ft_one_hot_encoder() with multiple columns", {
  sc <- testthat_spark_connection()
  df <- tibble(
    id = 0:5L,
    input1 = c(0, 1, 2, 0, 0, 2),
    input2 = c(2, 3, 0, 1, 0, 2)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  if (spark_version(sc) < "3.0.0") {
    expect_error(
      df_tbl %>%
        ft_one_hot_encoder(c("input1", "input2"), c("output1", "output2"))
    )
  } else {
    expect_identical(
      df_tbl %>%
        ft_one_hot_encoder(c("input1", "input2"), c("output1", "output2")) %>%
        colnames(),
      c("id", "input1", "input2", "output1", "output2")
    )
  }
})

test_that("ft_one_hot_encoder() works with ml pipeline", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.3.0") {
    pipeline <- ml_pipeline(sc) %>%
      ft_string_indexer("Species", "indexed") %>%
      ft_one_hot_encoder("indexed", "encoded")
  } else {
    pipeline <- ml_pipeline(sc) %>%
      ft_string_indexer(
        "Species",
        "indexed",
        string_order_type = "alphabetDesc"
      ) %>%
      ft_one_hot_encoder("indexed", "encoded")
  }

  expect_warning_on_arrow(
    f_o <- pipeline %>%
      ml_fit_and_transform(iris_tbl) %>%
      pull(encoded) %>%
      unique()
  )

  expect_setequal(
    f_o,
    list(c(0, 0), c(1, 0), c(0, 1))
  )
})

test_that("ft_one_hot_encoder_estimator() default params", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_one_hot_encoder_estimator)
})

test_that("ft_one_hot_encoder_estimator() param setting", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo", "foo1"),
    output_cols = c("bar", "bar1"),
    drop_last = FALSE
  )
  test_param_setting(sc, ft_one_hot_encoder_estimator, test_args)
})

test_that("ft_one_hot_encoder_estimator() works", {
  test_requires_version(min_version = "2.3.0", max_version = "3.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_equal(
    iris_tbl %>%
      ft_string_indexer("Species", "indexed") %>%
      ft_one_hot_encoder_estimator("indexed", "encoded") %>%
      pull(encoded) %>%
      unique(),
    list(c(0, 0), c(1, 0), c(0, 1))
  )
})

test_clear_cache()
