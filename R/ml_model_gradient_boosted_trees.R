#' @export
ml_gradient_boosted_trees <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
  type = c("auto", "regression", "classification"),
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  loss_type = c("auto", "logistic", "squared", "absolute"),
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gradient_boosted_trees_"), ...
) {
  UseMethod("ml_gradient_boosted_trees")
}

#' @export
ml_gradient_boosted_trees.tbl_spark <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
  type = c("auto", "regression", "classification"),
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  loss_type = c("auto", "logistic", "squared", "absolute"),
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gradient_boosted_trees_"), ...
) {

  ml_formula_transformation()
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  response_type <- schema[[response_col]]$type

  type <- rlang::arg_match(type)
  model_type <- if (!identical(type, "auto")) type else {
    if (response_type %in% c("DoubleType", "IntegerType"))
      "regression"
    else
      "classification"
  }

  loss_type <- rlang::arg_match(loss_type)
  loss_type <- if (identical(loss_type, "auto")) {
    if (identical(model_type, "classification")) "logistic" else "squared"
  } else if (identical(model_type, "regression")) {
    if (!loss_type %in% c("squared", "absolute"))
      stop("'loss_type' must be 'squared' or 'absolute' for regression")
    loss_type
  } else {
    if (!identical(loss_type, "logistic"))
      stop("'loss_type' must be 'logistic' for classification")
    loss_type
  }

  if (spark_version(spark_connection(x)) < "2.2.0" && !is.null(thresholds))
    stop("thresholds is only supported for GBT in Spark 2.2.0+")

  routine <- switch(model_type,
                    regression = ml_gbt_regressor,
                    classification = ml_gbt_classifier)

  args <- c(as.list(environment()), list(...))
  do.call(routine, args)
}
