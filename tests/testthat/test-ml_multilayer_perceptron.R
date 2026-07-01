# ---------------------------------------------------------------------------
# Connection-free contract tests. The live tests below are version-capped at
# Spark <= 3.3, so they never run on CI's Spark 3.5/4.1 -- this file would be 0%
# without these. We mock the JVM-facing layer (spark_pipeline_stage / invoke /
# jobj_set_param / new_ml_*) and assert the calls the R code emits, covering the
# construction chain, the version-gated constructors, and the dispatchers without
# a live MLP fit. Placed ABOVE the top-level test_requires_version() gate so they
# always run. (force(jobj) in each mock keeps the lazy magrittr chain in order.)
# ---------------------------------------------------------------------------
mlp_fake_sc <- structure(list(), class = "spark_connection")

capture_mlp_calls <- function(expr) {
  calls <- list()
  rec <- function(e) {
    calls[[length(calls) + 1]] <<- e
    "<jobj>"
  }
  with_mocked_bindings(
    spark_pipeline_stage = function(sc, class, uid, ...) {
      rec(list(
        call = "spark_pipeline_stage",
        class = class,
        uid = uid,
        params = list(...)
      ))
    },
    jobj_set_param = function(jobj, setter, ...) {
      force(jobj)
      rec(list(call = setter))
    },
    invoke = function(jobj, method, ...) {
      force(jobj)
      rec(list(call = method))
    },
    invoke_static = function(sc, class, method, ...) rec(list(call = method)),
    spark_version = function(x) numeric_version("3.5.0"),
    spark_connection = function(x, ...) mlp_fake_sc,
    new_ml_multilayer_perceptron_classifier = function(jobj) {
      force(jobj)
      rec(list(call = "new_ml_multilayer_perceptron_classifier"))
    },
    .package = "sparklyr",
    force(expr)
  )
  calls
}

test_that("ml_multilayer_perceptron_classifier() builds the classifier stage", {
  calls <- capture_mlp_calls(
    ml_multilayer_perceptron_classifier(
      mlp_fake_sc,
      layers = c(4, 3),
      max_iter = 50L,
      step_size = 0.01,
      tol = 1e-5,
      block_size = 256L,
      solver = "gd",
      seed = 42L,
      uid = "mlp_1"
    )
  )

  expect_equal(
    vapply(calls, `[[`, character(1), "call"),
    c(
      "spark_pipeline_stage",
      "setLayers",
      "setMaxIter",
      "setStepSize",
      "setTol",
      "setBlockSize",
      "setSolver",
      "setSeed",
      "setThresholds",
      "setProbabilityCol",
      "setRawPredictionCol",
      "new_ml_multilayer_perceptron_classifier"
    )
  )
  stage <- calls[[1]]
  expect_equal(
    stage$class,
    "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
  )
  expect_equal(stage$uid, "mlp_1")
  expect_equal(
    stage$params,
    list(
      features_col = "features",
      label_col = "label",
      prediction_col = "prediction"
    )
  )
})

test_that("ml_multilayer_perceptron_classifier() sets initial weights when given", {
  calls <- capture_mlp_calls(
    ml_multilayer_perceptron_classifier(
      mlp_fake_sc,
      layers = c(4, 3),
      initial_weights = 1:5,
      uid = "mlp_2"
    )
  )
  expect_true(
    "setInitialWeights" %in% vapply(calls, `[[`, character(1), "call")
  )
})

test_that("new_ml_multilayer_perceptron_classifier() picks class by Spark version", {
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    spark_version = function(x) numeric_version("3.5.0"),
    new_ml_probabilistic_classifier = function(jobj, class) {
      list(kind = "prob", class = class)
    },
    new_ml_predictor = function(jobj, class) list(kind = "pred"),
    .package = "sparklyr",
    {
      res <- new_ml_multilayer_perceptron_classifier("<jobj>")
      expect_equal(res$kind, "prob")
      expect_equal(res$class, "ml_multilayer_perceptron_classifier")
    }
  )
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    spark_version = function(x) numeric_version("2.2.0"),
    new_ml_probabilistic_classifier = function(jobj, class) list(kind = "prob"),
    new_ml_predictor = function(jobj, class) list(kind = "pred"),
    .package = "sparklyr",
    expect_equal(new_ml_multilayer_perceptron_classifier("<jobj>")$kind, "pred")
  )
})

test_that("new_ml_..._classification_model() wraps a probabilistic model (>= 2.3)", {
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    spark_version = function(x) numeric_version("3.5.0"),
    invoke = function(jobj, method, ...) {
      if (method == "layers") c(4L, 10L, 3L) else NULL
    },
    read_spark_vector = function(jobj, name) c(0.1, 0.2),
    new_ml_probabilistic_classification_model = function(
      jobj,
      weights,
      layers,
      class
    ) {
      list(layers = layers, weights = weights, class = class)
    },
    .package = "sparklyr",
    {
      res <- new_ml_multilayer_perceptron_classification_model("<jobj>")
      expect_equal(res$layers, c(4L, 10L, 3L))
      expect_equal(res$class, "ml_multilayer_perceptron_classification_model")
    }
  )
})

test_that("new_ml_..._classification_model() resolves jobj-wrapped layers", {
  resolved <- FALSE
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    spark_version = function(x) numeric_version("3.5.0"),
    invoke = function(jobj, method, ...) {
      if (method == "layers") {
        structure(list(), class = "spark_jobj")
      } else if (method == "%>%") {
        resolved <<- TRUE
        c(4L, 3L)
      } else {
        NULL
      }
    },
    read_spark_vector = function(jobj, name) c(0.1),
    new_ml_probabilistic_classification_model = function(
      jobj,
      weights,
      layers,
      class
    ) {
      list(layers = layers)
    },
    .package = "sparklyr",
    {
      res <- new_ml_multilayer_perceptron_classification_model("<jobj>")
      expect_true(resolved)
      expect_equal(res$layers, c(4L, 3L))
    }
  )
})

test_that("new_ml_..._classification_model() builds a prediction model (< 2.3)", {
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    spark_version = function(x) numeric_version("2.2.0"),
    invoke = function(jobj, method, ...) {
      if (method == "layers") c(4L, 3L) else NULL
    },
    read_spark_vector = function(jobj, name) c(0.1, 0.2),
    new_ml_prediction_model = function(jobj, layers, weights, class) {
      list(layers = layers, class = class)
    },
    .package = "sparklyr",
    {
      res <- new_ml_multilayer_perceptron_classification_model("<jobj>")
      expect_equal(res$layers, c(4L, 3L))
      expect_equal(res$class, "ml_multilayer_perceptron_classification_model")
    }
  )
})

test_that("ml_multilayer_perceptron_classifier(ml_pipeline) adds the stage", {
  forwarded <- NULL
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    ml_multilayer_perceptron_classifier.spark_connection = function(
      x,
      layers,
      ...
    ) {
      forwarded <<- layers
      "<stage>"
    },
    ml_add_stage = function(x, stage) list(stage = stage),
    .package = "sparklyr",
    {
      res <- ml_multilayer_perceptron_classifier(
        structure(list(), class = "ml_pipeline"),
        layers = c(4, 3),
        uid = "u"
      )
      expect_equal(res$stage, "<stage>")
      expect_equal(forwarded, c(4, 3))
    }
  )
})

test_that("ml_multilayer_perceptron_classifier(tbl_spark) fits or builds a model", {
  # no formula -> ml_fit()
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    ml_standardize_formula = function(formula, response, features) NULL,
    ml_multilayer_perceptron_classifier.spark_connection = function(x, ...) {
      "<stage>"
    },
    ml_fit = function(stage, x) list(fitted = stage),
    .package = "sparklyr",
    {
      res <- ml_multilayer_perceptron_classifier(
        structure(list(), class = "tbl_spark"),
        layers = c(4, 3)
      )
      expect_equal(res$fitted, "<stage>")
    }
  )
  # formula -> ml_construct_model_supervised()
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    ml_standardize_formula = function(formula, response, features) {
      "Species ~ ."
    },
    ml_multilayer_perceptron_classifier.spark_connection = function(x, ...) {
      "<stage>"
    },
    ml_construct_model_supervised = function(
      constructor,
      predictor,
      formula,
      ...
    ) {
      list(model = predictor, formula = formula)
    },
    .package = "sparklyr",
    {
      res <- ml_multilayer_perceptron_classifier(
        structure(list(), class = "tbl_spark"),
        formula = "Species ~ .",
        layers = c(4, 3)
      )
      expect_equal(res$model, "<stage>")
    }
  )
})

test_that("ml_multilayer_perceptron() is a deprecated alias", {
  with_mocked_bindings(
    spark_connection = function(x, ...) mlp_fake_sc,
    ml_multilayer_perceptron_classifier.spark_connection = function(x, ...) {
      "<stage>"
    },
    ml_add_stage = function(x, stage) "<pipeline>",
    .package = "sparklyr",
    expect_warning(
      ml_multilayer_perceptron(
        structure(list(), class = "ml_pipeline"),
        layers = c(4, 3)
      ),
      "deprecated"
    )
  )
})

test_that("new_ml_model_multilayer_perceptron_classification() forwards to constructor", {
  with_mocked_bindings(
    new_ml_model_classification = function(
      pipeline_model,
      formula,
      dataset,
      label_col,
      features_col,
      predicted_label_col,
      class
    ) {
      list(class = class)
    },
    .package = "sparklyr",
    {
      res <- new_ml_model_multilayer_perceptron_classification(
        "<pm>",
        "f",
        "ds",
        "label",
        "features",
        "pred"
      )
      expect_equal(res$class, "ml_model_multilayer_perceptron_classification")
    }
  )
})

test_requires_version(min_version = "2.4", max_version = "3.3")

skip_connection("ml_multilayer_perceptron")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_multilayer_perceptron_classifier() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_multilayer_perceptron_classifier)
})

test_that("ml_multilayer_perceptron_classifier() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    layers = c(6, 32, 64, 32),
    max_iter = 50,
    step_size = 0.01,
    tol = 1e-5,
    block_size = 256,
    solver = "gd",
    seed = 34534,
    initial_weights = 1:10,
    features_col = "fosadf",
    label_col = "wefwfe"
  )
  test_param_setting(sc, ml_multilayer_perceptron_classifier, test_args)
})

test_that("ml_multilayer_perceptron returns correct number of weights", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  mlp <- ml_multilayer_perceptron_classifier(
    iris_tbl,
    formula = "Species ~ .",
    seed = 42,
    layers = c(4, 10, 3)
  )
  expect_equal(length(mlp$model$weights), 4 * 10 + 10 + 10 * 3 + 3)
})

test_clear_cache()
