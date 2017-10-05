#' @export
ml_random_forest <- function(
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
  feature_subset_strategy = "auto",
  impurity = "auto",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_"), ...
) {
  UseMethod("ml_random_forest")
}

#' @export
ml_random_forest.tbl_spark <- function(
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
  feature_subset_strategy = "auto",
  impurity = "auto",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_"), ...
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

  impurity <- if (identical(impurity, "auto")) {
    if (identical(model_type, "regression")) "variance" else "gini"
  } else if (identical(model_type, "classification")) {
    if (!impurity %in% c("gini", "entropy"))
      stop("'impurity' must be 'gini' or 'entropy' for classification")
    impurity
  } else {
    if (!identical(impurity, "variance"))
      stop("'impurity' must be 'variance' for regression")
    impurity
  }

  if (rlang::has_name(rlang::dots_list(...), "col.sample.rate")) {
    col.sample.rate <- rlang::dots_list(...)[["col.sample.rate"]]
    sc <- spark_connection(x)
    if (!(col.sample.rate > 0 && col.sample.rate <= 1))
      stop("'col.sample.rate' must be in (0, 1]")
    col.sample.rate <- if (spark_version(sc) < "2.0.0") {
      if (col.sample.rate == 1)
        "all"
      else {
        # Prior to Spark 2.0.0, random forest does not support arbitrary
        #   column sampling rates. So we map the input to one of the supported
        #   strategies: "onethird", "sqrt", or "log2".
        k <- length(features)
        strategies <- dplyr::data_frame(strategy = c("onethird", "sqrt", "log2"),
                                        rate = c(1/3, sqrt(k)/k, log2(k)/k)) %>%
          arrange(!! rlang::sym("rate"))
        strategy <- strategies[["strategy"]][
          max(findInterval(col.sample.rate, strategies[["rate"]]), 1)]
        message("* Using feature subsetting strategy: ", strategy)
        strategy
      }
    } else {
      ensure_scalar_character(format(col.sample.rate, nsmall = 1L))
    }
    assign("col.sample.rate", col.sample.rate, envir = parent.frame())
  }

  routine <- switch(model_type,
                    regression = ml_random_forest_regressor,
                    classification = ml_random_forest_classifier)

  args <- c(as.list(environment()), list(...))
  do.call(routine, args)
}
