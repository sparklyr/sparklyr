#' Spark ML -- Decision Trees
#'
#' Perform regression or classification using decision trees.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-decision-trees-impurity
#' @template roxlate-ml-decision-trees-max-bins
#' @template roxlate-ml-decision-trees-max-depth
#' @template roxlate-ml-decision-trees-min-info-gain
#' @template roxlate-ml-decision-trees-min-rows
#' @template roxlate-ml-decision-trees-thresholds
#' @template roxlate-ml-decision-trees-type
#' @template roxlate-ml-decision-trees-seed
#' @template roxlate-ml-checkpoint-interval
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#' @template roxlate-ml-decision-trees-cache-node-ids
#' @template roxlate-ml-decision-trees-max-memory
#' @family Spark ML routines
#'
#' @export
ml_decision_tree <- function(x,
                             response,
                             features,
                             impurity = c("auto", "gini", "entropy", "variance"),
                             max.bins = 32L,
                             max.depth = 5L,
                             min.info.gain = 0,
                             min.rows = 1L,
                             type = c("auto", "regression", "classification"),
                             thresholds = NULL,
                             seed = NULL,
                             checkpoint.interval = 10L,
                             cache.node.ids = FALSE,
                             max.memory = 256L,
                             ml.options = ml_options(),
                             ...)
{
  ml_backwards_compatibility_api()

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    df,
    response = response,
    features = features,
    intercept = NULL,
    envir = environment(),
    categorical.transformations = categorical.transformations,
    ml.options = ml.options
  )

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  min.info.gain <- ensure_scalar_double(min.info.gain)
  min.rows <- ensure_scalar_integer(min.rows)
  type <- match.arg(type)
  only.model <- ensure_scalar_boolean(ml.options$only.model)
  thresholds <- if (!is.null(thresholds)) lapply(thresholds, ensure_scalar_double)
  seed <- ensure_scalar_integer(seed, allow.null = TRUE)
  checkpoint.interval <- ensure_scalar_integer(checkpoint.interval)
  cache.node.ids <- ensure_scalar_boolean(cache.node.ids)
  max.memory <- ensure_scalar_integer(max.memory)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- sdf_schema(df)
  responseType <- schema[[response]]$type

  modelType <- if (identical(type, "regression"))
    "regression"     else if (identical(type, "classification"))
    "classification" else if (responseType %in% c("DoubleType", "IntegerType"))
    "regression"     else
    "classification"

  envir$model <- ifelse(identical(modelType, "regression"),
                        "org.apache.spark.ml.regression.DecisionTreeRegressor",
                        "org.apache.spark.ml.classification.DecisionTreeClassifier")

  dt <- invoke_new(sc, envir$model)

  impurity <- rlang::arg_match(impurity)
  impurity <- if (identical(impurity, "auto")) {
    ifelse(identical(modelType, "regression"), "variance", "gini")
  } else if (identical(modelType, "classification")) {
    if (!impurity %in% c("gini", "entropy"))
      stop("'impurity' must be 'gini' or 'entropy' for classification")
    impurity
  } else {
    if (!identical(impurity, "variance"))
      stop("'impurity' must be 'variance' for regression")
    impurity
  }

  model <- dt %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth) %>%
    invoke("setMinInfoGain", min.info.gain) %>%
    invoke("setMinInstancesPerNode", min.rows) %>%
    invoke("setCheckpointInterval", checkpoint.interval) %>%
    invoke("setCacheNodeIds", cache.node.ids) %>%
    invoke("setMaxMemoryInMB", max.memory)

  if (!is.null(thresholds))
    model <- invoke(model, "setThresholds", thresholds)

  if (!is.null(seed))
    model <- invoke(model, "setSeed", seed)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

  fit <- model %>%
    invoke("fit", tdf)

  ml_model("decision_tree", fit,
           features = features,
           response = response,
           impurity = impurity,
           max.bins = max.bins,
           max.depth = max.depth,
           min.info.gain = min.info.gain,
           min.rows = min.rows,
           thresholds = unlist(thresholds),
           seed = seed,
           checkpoint.interval = checkpoint.interval,
           categorical.transformations = categorical.transformations,
           cache.node.ids = cache.node.ids,
           max.memory = max.memory,
           data = df,
           ml.options = ml.options,
           model.parameters = as.list(envir)
  )
}

#' @export
print.decision_tree <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
