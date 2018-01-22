#' Frequent Pattern Mining -- FPGrowth
#'
#' A parallel FP-growth algorithm to mine frequent itemsets.
#'
#' @template roxlate-ml-x
#' @param items_col Items column name. Default: "items"
#' @param min_confidence Minimal confidence for generating Association Rule.
#'   \code{min_confidence} will not affect the mining for frequent itemsets, but
#'   will affect the association rules generation. Default: 0.8
#' @param min_support Minimal support level of the frequent pattern. [0.0, 1.0].
#'   Any pattern that appears more than (min_support * size-of-the-dataset) times
#'    will be output in the frequent itemsets. Default: 0.3
#' @template roxlate-ml-prediction-col
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @name ml_fpgrowth
#' @export
ml_fpgrowth <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"), ...
) {
  UseMethod("ml_fpgrowth")
}

#' @export
ml_fpgrowth.spark_connection <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.fpm.FPGrowth", uid) %>%
    invoke("setItemsCol", items_col) %>%
    invoke("setMinConfidence", min_confidence) %>%
    invoke("setMinSupport", min_support) %>%
    invoke("setPredictionCol", prediction_col)

  new_ml_fpgrowth(jobj)
}

#' @export
ml_fpgrowth.ml_pipeline <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"), ...) {

  estimator <- ml_new_stage_modified_args()
  ml_add_stage(x, estimator)
}

#' @export
ml_fpgrowth.tbl_spark <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"), ...) {

  estimator <- ml_new_stage_modified_args()

  estimator %>%
    ml_fit(x)

}

# Validator
ml_validator_fpgrowth <- function(args, nms) {
  args %>%
    ml_validate_args({
      items_col <- ensure_scalar_character(items_col)
      min_confidence <- ensure_scalar_double(min_confidence)
      min_support <- ensure_scalar_double(min_support)
      prediction_col <- ensure_scalar_character(prediction_col)
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_fpgrowth <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_fpgrowth")
}

new_ml_fpgrowth_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    association_rules = invoke(jobj, "associationRules") %>%
      sdf_register(),
    freq_itemsets = invoke(jobj, "freqItemsets") %>%
      sdf_register(),
    subclass = "ml_fpgrowth_model")
}

#' @rdname ml_fpgrowth
#' @param model A fitted FPGrowth model returned by \code{ml_fpgrowth()}
#' @export
ml_association_rules <- function(model) {
  model$association_rules
}

#' @rdname ml_fpgrowth
#' @export
ml_freq_itemsets <- function(model) {
  model$freq_itemsets
}
