#' Spark ML -- Gradient-Boosted Tree
#'
#' Perform regression or classification using gradient-boosted trees.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-decision-trees-impurity
#' @template roxlate-ml-decision-trees-loss-type
#' @template roxlate-ml-decision-trees-max-bins
#' @template roxlate-ml-decision-trees-max-depth
#' @template roxlate-ml-decision-trees-num-trees
#' @template roxlate-ml-decision-trees-min-info-gain
#' @template roxlate-ml-decision-trees-min-rows
#' @template roxlate-ml-decision-trees-type
#' @template roxlate-ml-decision-trees-seed
#' @template roxlate-ml-decision-trees-learn-rate
#' @template roxlate-ml-decision-trees-sample-rate
#' @template roxlate-ml-decision-trees-thresholds
#' @template roxlate-ml-checkpoint-interval
#' @template roxlate-ml-decision-trees-cache-node-ids
#' @template roxlate-ml-decision-trees-max-memory
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_gradient_boosted_trees <- function(x,
                                      response,
                                      features,
                                      impurity = c("auto", "gini", "entropy", "variance"),
                                      loss.type = c("auto", "logistic", "squared", "absolute"),
                                      max.bins = 32L,
                                      max.depth = 5L,
                                      num.trees = 20L,
                                      min.info.gain = 0,
                                      min.rows = 1L,
                                      learn.rate = 0.1,
                                      sample.rate = 1.0,
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
    x = df,
    response = response,
    features = features,
    intercept = NULL,
    envir = environment(),
    categorical.transformations = categorical.transformations,
    ml.options = ml.options
  )

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  num.trees <- ensure_scalar_integer(num.trees)
  if (num.trees < 1) stop("num.trees must be >= 1")
  min.info.gain <- ensure_scalar_double(min.info.gain)
  min.rows <- ensure_scalar_integer(min.rows)
  only.model <- ensure_scalar_boolean(ml.options$only.model)
  type <- match.arg(type)
  learn.rate <- ensure_scalar_double(learn.rate)
  sample.rate <- ensure_scalar_double(sample.rate)
  seed <- ensure_scalar_integer(seed, allow.null = TRUE)
  thresholds <- if (!is.null(thresholds)) lapply(thresholds, ensure_scalar_double)
  # https://issues.apache.org/jira/browse/SPARK-14975
  if (spark_version(sc) < "2.2.0" && !is.null(thresholds))
    stop("thresholds is only supported for GBT in Spark 2.2.0+")
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
                        "org.apache.spark.ml.regression.GBTRegressor",
                        "org.apache.spark.ml.classification.GBTClassifier")

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

  loss.type <- rlang::arg_match(loss.type)
  loss.type <- if (identical(loss.type, "auto")) {
    ifelse(identical(modelType, "classification"), "logistic", "squared")
  } else if (identical(modelType, "regression")) {
    if (!loss.type %in% c("squared", "absolute"))
      stop("'loss.type' must be 'squared' or 'absolute' for regression")
    loss.type
  } else {
    if (!identical(loss.type, "logistic"))
      stop("'loss.type' must be 'logistic' for classification")
    loss.type
  }


  gbt <- invoke_new(sc, envir$model)

  model <- gbt %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setLossType", loss.type) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth) %>%
    invoke("setMinInfoGain", min.info.gain) %>%
    invoke("setMinInstancesPerNode", min.rows) %>%
    invoke("setMaxIter", num.trees) %>%
    invoke("setStepSize", learn.rate) %>%
    invoke("setSubsamplingRate", sample.rate) %>%
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

  ml_model("gradient_boosted_trees", fit,
    features = features,
    response = response,
    impurity = impurity,
    loss.type = loss.type,
    max.bins = max.bins,
    max.depth = max.depth,
    num.trees = num.trees,
    trees = invoke(fit, "trees"),
    min.info.gain = min.info.gain,
    min.rows = min.rows,
    learn.rate = learn.rate,
    sample.rate = sample.rate,
    thresholds = unlist(thresholds),
    seed = seed,
    checkpoint.interval = checkpoint.interval,
    cache.node.ids = cache.node.ids,
    max.memory = max.memory,
    data = df,
    ml.options = ml.options,
    categorical.transformations = categorical.transformations,
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_gradient_boosted_trees <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
