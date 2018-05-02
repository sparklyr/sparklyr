#' Spark ML -- Naive-Bayes
#'
#' Naive Bayes Classifiers. It supports Multinomial NB (see \href{http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html}{here}) which can handle finitely supported discrete data. For example, by converting documents into TF-IDF vectors, it can be used for document classification. By making every vector a binary (0/1) data, it can also be used as Bernoulli NB (see \href{http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html}{here}). The input feature values must be nonnegative.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-probabilistic-classifier-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-formula-params
#' @param model_type The model type. Supported options: \code{"multinomial"}
#'   and \code{"bernoulli"}. (default = \code{multinomial})
#' @param smoothing The (Laplace) smoothing parameter. Defaults to 1.
#' @param weight_col (Spark 2.1.0+) Weight column name. If this is not set or empty, we treat all instance weights as 1.0.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' nb_model <- iris_training %>%
#'   ml_naive_bayes(Species ~ .)
#'
#' pred <- sdf_predict(iris_test, nb_model)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#'
#' @export
ml_naive_bayes <- function(
  x,
  formula = NULL,
  model_type = "multinomial",
  smoothing = 1,
  thresholds = NULL,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("naive_bayes_"), ...
) {
  UseMethod("ml_naive_bayes")
}

#' @export
ml_naive_bayes.spark_connection <- function(
  x,
  formula = NULL,
  model_type = "multinomial",
  smoothing = 1,
  thresholds = NULL,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("naive_bayes_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.NaiveBayes", uid,
    features_col, label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setSmoothing", smoothing) %>%
    invoke("setModelType", model_type)

  if(!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(weight_col))
    jobj <- jobj_set_param(jobj, "setWeightCol", weight_col, NULL, "2.1.0")

  new_ml_naive_bayes(jobj)
}

#' @export
ml_naive_bayes.ml_pipeline <- function(
  x,
  formula = NULL,
  model_type = "multinomial",
  smoothing = 1,
  thresholds = NULL,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("naive_bayes_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_naive_bayes.tbl_spark <- function(
  x,
  formula = NULL,
  model_type = "multinomial",
  smoothing = 1,
  thresholds = NULL,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("naive_bayes_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor, formula, features_col, label_col,
      "classification", new_ml_model_naive_bayes,
      predicted_label_col
    )
  }
}

# Validator
ml_validator_naive_bayes <- function(args, nms) {
  old_new_mapping <- list(
    lambda = "smoothing"
  )
  args %>%
    ml_validate_args({
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)

      smoothing <- ensure_scalar_double(smoothing)
      if (!rlang::is_null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
      model_type <- rlang::arg_match(model_type, c("multinomial", "bernoulli"))
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_naive_bayes <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_naive_bayes")
}

new_ml_naive_bayes_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    num_features = invoke(jobj, "numFeatures"),
    num_classes = invoke(jobj, "numClasses"),
    pi = read_spark_vector(jobj, "pi"),
    theta = read_spark_matrix(jobj, "theta"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_naive_bayes_model")
}

new_ml_model_naive_bayes <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

    pi <- model$pi
    names(pi) <- index_labels

    theta <- model$theta
    rownames(theta) <- index_labels
    colnames(theta) <- feature_names


  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_naive_bayes",
    !!! list(pi = pi,
    theta = theta,
    .features = feature_names,
    .index_labels = index_labels)
  )
}

# Generic implementations

#' @export
print.ml_model_naive_bayes <- function(x, ...) {
  printf("A-priority probabilities:\n")
  print(exp(x$pi))
  print_newline()

  printf("Conditional probabilities:\n")
  print(exp(x$theta))
  print_newline()

  x
}

#' @export
summary.ml_model_naive_bayes <- function(object, ...) {
  print(object, ...)
}
