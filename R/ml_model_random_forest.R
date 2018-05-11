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
#' @template roxlate-ml-feature-subset-strategy
#' @param num_trees Number of trees to train (>= 1). If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @name ml_random_forest
NULL

#' @rdname ml_random_forest
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_random_forest} is a wrapper around \code{ml_random_forest_regressor.tbl_spark} and \code{ml_random_forest_classifier.tbl_spark} and calls the appropriate method based on model type.
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
#' rf_model <- iris_training %>%
#'   ml_random_forest(Species ~ ., type = "classification")
#'
#' pred <- sdf_predict(iris_test, rf_model)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
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
  args$response <- NULL
  args$features <- NULL
  do.call(routine, args)
}
