#' Spark ML -- Random Forest
#'
#' Perform classification and regression using random forests.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-old-feature-response
#' @param impurity Criterion used for information gain calculation. Supported: "entropy"
#'   and "gini" (default) for classification and "variance" (default) for regression. For
#'   \code{ml_decision_tree}, setting \code{"auto"} will default to the appropriate
#'   criterion based on model type.
#' @param feature_subset_strategy The number of features to consider for splits at each tree node. See details for options.
#' @param num_trees Number of trees to train (>= 1). If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @details The supported options for \code{feature_subset_strategy} are
#'   \itemize{
#'     \item \code{"auto"}: Choose automatically for task: If \code{num_trees == 1}, set to \code{"all"}. If \code{num_trees > 1} (forest), set to \code{"sqrt"} for classification and to \code{"onethird"} for regression.
#'     \item \code{"all"}: use all features
#'     \item \code{"onethird"}: use 1/3 of the features
#'     \item \code{"sqrt"}: use use sqrt(number of features)
#'     \item \code{"log2"}: use log2(number of features)
#'     \item \code{"n"}: when \code{n} is in the range (0, 1.0], use n * number of features. When \code{n} is in the range (1, number of features), use \code{n} features. (default = \code{"auto"})
#'     }
#' @name ml_random_forest
NULL

#' @rdname ml_random_forest
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_random_forest} is a wrapper around \code{ml_random_forest_regressor.tbl_spark} and \code{ml_random_forest_classifier.tbl_spark} and calls the appropriate method based on model type.
#' @export
ml_random_forest <- function(
  x,
  formula = NULL,
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
  uid = random_string("random_forest_"),
  response = NULL,
  features = NULL, ...
) {

  ml_formula_transformation()
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  if (!response_col %in% names(schema))
    stop(paste0(response_col, " is not a column in the input dataset"))

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
      if (col.sample.rate == 1) # nocov start
        "all"
      else {
        # Prior to Spark 2.0.0, random forest does not support arbitrary
        #   column sampling rates. So we map the input to one of the supported
        #   strategies: "onethird", "sqrt", or "log2".
        k <- gsub("^.+~", "", formula) %>%
          trimws() %>%
          strsplit("\\+") %>%
          rlang::flatten_chr() %>%
          length()
        strategies <- dplyr::data_frame(strategy = c("onethird", "sqrt", "log2"),
                                        rate = c(1/3, sqrt(k)/k, log2(k)/k)) %>%
          dplyr::arrange(!! rlang::sym("rate"))
        strategy <- strategies %>%
          dplyr::pull("strategy") %>%
          `[[`(max(findInterval(col.sample.rate, strategies[["rate"]]), 1))
        message("* Using feature subsetting strategy: ", strategy)
        strategy
      } # nocov end
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
