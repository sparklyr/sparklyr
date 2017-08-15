#' Spark ML -- Random Forests
#'
#' Perform regression or classification using random forests with a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-decision-trees-col-sample-rate
#' @template roxlate-ml-decision-trees-impurity
#' @template roxlate-ml-decision-trees-max-bins
#' @template roxlate-ml-decision-trees-max-depth
#' @template roxlate-ml-decision-trees-min-info-gain
#' @template roxlate-ml-decision-trees-min-rows
#' @template roxlate-ml-decision-trees-num-trees
#' @template roxlate-ml-decision-trees-thresholds
#' @template roxlate-ml-decision-trees-type
#' @template roxlate-ml-decision-trees-sample-rate
#' @template roxlate-ml-decision-trees-seed
#' @template roxlate-ml-checkpoint-interval
#' @template roxlate-ml-decision-trees-cache-node-ids
#' @template roxlate-ml-decision-trees-max-memory
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_random_forest <- function(x,
                             response,
                             features,
                             col.sample.rate = NULL,
                             impurity = c("auto", "gini", "entropy", "variance"),
                             max.bins = 32L,
                             max.depth = 5L,
                             min.info.gain = 0,
                             min.rows = 1L,
                             num.trees = 20L,
                             sample.rate = 1.0,
                             thresholds = NULL,
                             seed = NULL,
                             type = c("auto", "regression", "classification"),
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

  col.sample.rate <- ensure_scalar_double(col.sample.rate, allow.null = TRUE)
  if (!is.null(col.sample.rate)) {
    if (!(col.sample.rate > 0 && col.sample.rate <= 1))
      stop("'col.sample.rate' must be in (0, 1]")
    if (spark_version(sc) < "2.0.0") {
      if (col.sample.rate == 1)
        col.sample.rate <- "all"
      else {
        # Prior to Spark 2.0.0, random forest does not support arbitrary
        #   column sampling rates. So we map the input to one of the supported
        #   strategies: "onethird", "sqrt", or "log2".
        k <- length(features)
        strategies <- dplyr::data_frame(strategy = c("onethird", "sqrt", "log2"),
                                        rate = c(1/3, sqrt(k)/k, log2(k)/k)) %>%
          arrange(!! rlang::sym("rate"))
        col.sample.rate <- strategies[["strategy"]][
          max(findInterval(col.sample.rate, strategies[["rate"]]), 1)]
        message("* Using feature subsetting strategy: ", col.sample.rate)
      }
    } else {
      col.sample.rate <- ensure_scalar_character(format(col.sample.rate, nsmall = 1L))
    }
  } else {
    col.sample.rate <- "auto"
  }

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  min.info.gain <- ensure_scalar_double(min.info.gain)
  min.rows <- ensure_scalar_integer(min.rows)
  num.trees <- ensure_scalar_integer(num.trees)
  if (num.trees < 1) stop("num.trees must be >= 1")
  type <- match.arg(type)
  sample.rate <- ensure_scalar_double(sample.rate)
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
                        "org.apache.spark.ml.regression.RandomForestRegressor",
                        "org.apache.spark.ml.classification.RandomForestClassifier")

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

  rf <- invoke_new(sc, envir$model)

  model <- rf %>%
    invoke("setFeatureSubsetStrategy", col.sample.rate) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth) %>%
    invoke("setMinInfoGain", min.info.gain) %>%
    invoke("setMinInstancesPerNode", min.rows) %>%
    invoke("setNumTrees", num.trees) %>%
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

  featureImportances <- fit %>%
    invoke("featureImportances") %>%
    invoke("toArray")

  ml_model("random_forest", fit,
           features = features,
           response = response,
           col.sample.rate = col.sample.rate,
           impurity = impurity,
           max.bins = max.bins,
           max.depth = max.depth,
           min.info.gain = min.info.gain,
           min.rows = min.rows,
           num.trees = num.trees,
           thresholds = unlist(thresholds),
           sample.rate = sample.rate,
           seed = seed,
           feature.importances = featureImportances,
           trees = invoke(fit, "trees"),
           data = df,
           checkpoint.interval = checkpoint.interval,
           cache.node.ids = cache.node.ids,
           max.memory = max.memory,
           ml.options = ml.options,
           categorical.transformations = categorical.transformations,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_random_forest <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
