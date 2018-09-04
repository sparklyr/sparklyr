#' Spark ML -- Multilayer Perceptron
#'
#' Classification model based on the Multilayer Perceptron. Each layer has sigmoid activation function, output layer has softmax.
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @param layers A numeric vector describing the layers -- each element in the vector gives the size of a layer. For example, \code{c(4, 5, 2)} would imply three layers, with an input (feature) layer of size 4, an intermediate layer of size 5, and an output (class) layer of size 2.
#' @template roxlate-ml-tol
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-seed
#' @param step_size Step size to be used for each iteration of optimization (> 0).
#' @param block_size Block size for stacking input data in matrices to speed up the computation. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data. Recommended size is between 10 and 1000. Default: 128
#' @param initial_weights The initial weights of the model.
#' @param solver The solver algorithm for optimization. Supported options: "gd" (minibatch gradient descent) or "l-bfgs". Default: "l-bfgs"
#'
#' @examples
#' \dontrun{
#' sc <-  spark_connect(master = "local")
#'
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' mlp_model <- iris_training %>%
#'   ml_multilayer_perceptron_classifier(Species ~ ., layers = c(4,3,3))
#'
#' pred <- sdf_predict(iris_test, mlp_model)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }


#' @export
ml_multilayer_perceptron_classifier <- function(x, formula = NULL, layers = NULL, max_iter = 100,
                                                step_size = 0.03, tol = 1e-06, block_size = 128,
                                                solver = "l-bfgs", seed = NULL, initial_weights = NULL,
                                                features_col = "features", label_col = "label",
                                                prediction_col = "prediction",
                                                uid = random_string("multilayer_perceptron_classifier_"), ...) {
  UseMethod("ml_multilayer_perceptron_classifier")
}

#' @export
ml_multilayer_perceptron_classifier.spark_connection <- function(x, formula = NULL, layers = NULL, max_iter = 100,
                                                                 step_size = 0.03, tol = 1e-06, block_size = 128,
                                                                 solver = "l-bfgs", seed = NULL, initial_weights = NULL,
                                                                 features_col = "features", label_col = "label",
                                                                 prediction_col = "prediction",
                                                                 uid = random_string("multilayer_perceptron_classifier_"), ...) {

  .args <- list(
    layers = layers,
    max_iter = max_iter,
    step_size = step_size,
    tol = tol,
    block_size = block_size,
    solver = solver,
    seed = seed,
    initial_weights = initial_weights,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_multilayer_perceptron_classifier()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.classification.MultilayerPerceptronClassifier", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]) %>%
    maybe_set_param("setLayers", .args[["layers"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    maybe_set_param("setStepSize", .args[["step_size"]], "2.0.0", 0.03) %>%
    invoke("setTol", .args[["tol"]]) %>%
    invoke("setBlockSize", .args[["block_size"]]) %>%
    maybe_set_param("setSolver", .args[["solver"]], "2.0.0", "l-bfgs") %>%
    maybe_set_param("setSeed", .args[["seed"]])


  if(!is.null(initial_weights) && spark_version(x) >= "2.0.0")
    jobj <- invoke_static(spark_connection(jobj),
                          "sparklyr.MLUtils2",
                          "setInitialWeights",
                          jobj, .args[["initial_weights"]])

  new_ml_multilayer_perceptron_classifier(jobj)
}

#' @export
ml_multilayer_perceptron_classifier.ml_pipeline <- function(x, formula = NULL, layers = NULL, max_iter = 100,
                                                            step_size = 0.03, tol = 1e-06, block_size = 128,
                                                            solver = "l-bfgs", seed = NULL, initial_weights = NULL,
                                                            features_col = "features", label_col = "label",
                                                            prediction_col = "prediction",
                                                            uid = random_string("multilayer_perceptron_classifier_"), ...) {
  stage <- ml_multilayer_perceptron_classifier.spark_connection(
    x = spark_connection(x),
    formula = formula,
    layers = layers,
    max_iter = max_iter,
    step_size = step_size,
    tol = tol,
    block_size = block_size,
    solver = solver,
    seed = seed,
    initial_weights = initial_weights,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_multilayer_perceptron_classifier.tbl_spark <- function(x, formula = NULL, layers = NULL, max_iter = 100,
                                                          step_size = 0.03, tol = 1e-06, block_size = 128,
                                                          solver = "l-bfgs", seed = NULL, initial_weights = NULL,
                                                          features_col = "features", label_col = "label",
                                                          prediction_col = "prediction",
                                                          uid = random_string("multilayer_perceptron_classifier_"),
                                                          response = NULL, features = NULL,
                                                          predicted_label_col = "predicted_label", ...) {
  ml_formula_transformation()

  stage <- ml_multilayer_perceptron_classifier.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    layers = layers,
    max_iter = max_iter,
    step_size = step_size,
    tol = tol,
    block_size = block_size,
    solver = solver,
    seed = seed,
    initial_weights = initial_weights,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, stage, formula, features_col, label_col,
                         "classification",
                         new_ml_model_multilayer_perceptron_classification,
                         predicted_label_col)
  }
}

#' @rdname ml_multilayer_perceptron_classifier
#' @template roxlate-ml-old-feature-response
#' @details \code{ml_multilayer_perceptron()} is an alias for \code{ml_multilayer_perceptron_classifier()} for backwards compatibility.
#' @export
ml_multilayer_perceptron <- function(x, formula = NULL, layers, max_iter = 100, step_size = 0.03,
                                     tol = 1e-06, block_size = 128, solver = "l-bfgs", seed = NULL,
                                     initial_weights = NULL, features_col = "features", label_col = "label",
                                     prediction_col = "prediction",
                                     uid = random_string("multilayer_perceptron_classifier_"),
                                     response = NULL, features = NULL, ...) {
  .Deprecated("ml_multilayer_perceptron_classifier")
  UseMethod("ml_multilayer_perceptron_classifier")
}

ml_validator_multilayer_perceptron_classifier <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(iter.max = "max_iter"))
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["step_size"]] <- cast_scalar_double(.args[["step_size"]])
  .args[["layers"]] <- cast_nullable_integer_list(.args[["layers"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["block_size"]] <- cast_scalar_integer(.args[["block_size"]])
  .args[["initial_weights"]] <- cast_nullable_double_list(.args[["initial_weights"]])
  .args[["solver"]] <- cast_choice(.args[["solver"]], c("l-bfgs", "gd"))
  .args
}

new_ml_multilayer_perceptron_classifier <- function(jobj) {
  new_ml_classifier(jobj, subclass = "ml_multilayer_perceptron_classifier")
}

new_ml_multilayer_perceptron_classification_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    layers = invoke(jobj, "layers"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    weights = read_spark_vector(jobj, "weights"),
    subclass = "ml_multilayer_perceptron_classification_model")
}
