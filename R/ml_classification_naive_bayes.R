#' Spark ML -- Naive-Bayes
#'
#' Naive Bayes Classifiers. It supports Multinomial NB (see \href{https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html}{here}) which can handle finitely supported discrete data. For example, by converting documents into TF-IDF vectors, it can be used for document classification. By making every vector a binary (0/1) data, it can also be used as Bernoulli NB (see \href{http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html}{here}). The input feature values must be nonnegative.
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
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' nb_model <- iris_training %>%
#'   ml_naive_bayes(Species ~ .)
#'
#' pred <- ml_predict(nb_model, iris_test)
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
  uid = random_string("naive_bayes_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_naive_bayes")
}

ml_naive_bayes_impl <- function(
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
  predicted_label_col = "predicted_label",
  ...
) {
  weight_col <- param_min_version(x, weight_col, "2.1.0")

  ml_process_model(
    x = x,
    r_class = "ml_naive_bayes",
    ml_function = new_ml_model_naive_bayes,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    constructor_args = list(predicted_label_col = predicted_label_col),
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      smoothing = smoothing,
      model_type = model_type,
      thresholds = thresholds,
      weight_col = weight_col
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_naive_bayes.spark_connection <- ml_naive_bayes_impl

#' @export
ml_naive_bayes.ml_pipeline <- ml_naive_bayes_impl

#' @export
ml_naive_bayes.tbl_spark <- ml_naive_bayes_impl

# ---------------------------- Constructors ------------------------------------
new_ml_naive_bayes <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_naive_bayes")
}

new_ml_naive_bayes_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    pi = read_spark_vector(jobj, "pi"),
    theta = read_spark_matrix(jobj, "theta"),
    class = "ml_naive_bayes_model"
  )
}
