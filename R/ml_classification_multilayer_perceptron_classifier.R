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
#' @export
ml_multilayer_perceptron_classifier <- function(
  x,
  formula = NULL,
  layers,
  max_iter = 100L,
  step_size = 0.03,
  tol = 1e-06,
  block_size = 128L,
  solver = "l-bfgs",
  seed = NULL,
  initial_weights = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("multilayer_perceptron_classifier_"), ...
) {
  UseMethod("ml_multilayer_perceptron_classifier")
}

#' @export
ml_multilayer_perceptron_classifier.spark_connection <- function(
  x,
  formula = NULL,
  layers,
  max_iter = 100L,
  step_size = 0.03,
  tol = 1e-06,
  block_size = 128L,
  solver = "l-bfgs",
  seed = NULL,
  initial_weights = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("multilayer_perceptron_classifier_"), ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"

  jobj <- ml_new_predictor(x, class, uid, features_col,
                     label_col, prediction_col) %>%
    invoke("setMaxIter", max_iter) %>%
    jobj_set_param("setStepSize", step_size, 0.03, "2.0.0") %>%
    invoke("setLayers", layers) %>%
    invoke("setTol", tol) %>%
    invoke("setBlockSize", block_size) %>%
    jobj_set_param("setSolver", solver, "l-bfgs", "2.0.0")


  if(!rlang::is_null(initial_weights) && spark_version(x) >= "2.0.0")
    jobj <- invoke_static(spark_connection(jobj),
                          "sparklyr.MLUtils2",
                          "setInitialWeights",
                          jobj, initial_weights)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_multilayer_perceptron_classifier(jobj)
}

#' @export
ml_multilayer_perceptron_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  layers,
  max_iter = 100L,
  step_size = 0.03,
  tol = 1e-06,
  block_size = 128L,
  solver = "l-bfgs",
  seed = NULL,
  initial_weights = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("multilayer_perceptron_classifier_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_multilayer_perceptron_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  layers,
  max_iter = 100L,
  step_size = 0.03,
  tol = 1e-06,
  block_size = 128L,
  solver = "l-bfgs",
  seed = NULL,
  initial_weights = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("multilayer_perceptron_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "classification",
                         new_ml_model_multilayer_perceptron_classification,
                         predicted_label_col)
  }
}

#' @rdname ml_multilayer_perceptron_classifier
#' @template roxlate-ml-old-feature-response
#' @details \code{ml_multilayer_perceptron()} is an alias for \code{ml_multilayer_perceptron_classifier()} for backwards compatibility.
#' @export
ml_multilayer_perceptron <- function(
  x,
  formula = NULL,
  layers,
  max_iter = 100L,
  step_size = 0.03,
  tol = 1e-06,
  block_size = 128L,
  solver = "l-bfgs",
  seed = NULL,
  initial_weights = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("multilayer_perceptron_classifier_"),
  response = NULL,
  features = NULL, ...) {
  UseMethod("ml_multilayer_perceptron_classifier")
}

# Validator
ml_validator_multilayer_perceptron_classifier <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      iter.max = "max_iter"
    )
  )

  args %>%
    ml_validate_args({
      max_iter <- ensure_scalar_integer(max_iter)
      step_size <- ensure_scalar_double(step_size)
      layers <- lapply(layers, ensure_scalar_integer)
      if (!rlang::is_null(seed))
        seed <- ensure_scalar_integer(seed)
      tol <- ensure_scalar_double(tol)
      block_size <- ensure_scalar_integer(block_size)
      if (!rlang::is_null(initial_weights))
        initial_weights <- lapply(initial_weights, ensure_scalar_double)
      solver <- rlang::arg_match(solver, c("l-bfgs", "gd"))

    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_multilayer_perceptron_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_multilayer_perceptron_classifier")
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

new_ml_model_multilayer_perceptron_classification <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_multilayer_perceptron_classification",
    .features = feature_names,
    .index_labels = index_labels,
    .call = call
  )
}
