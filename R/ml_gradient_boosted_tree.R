#' Spark ML -- Gradient-Boosted Tree
#'
#' Perform regression or classification using gradient-boosted trees.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-decision-trees-max-bins
#' @template roxlate-ml-decision-trees-max-depth
#' @template roxlate-ml-decision-trees-type
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_gradient_boosted_trees <- function(x,
                                      response,
                                      features,
                                      max.bins = 32L,
                                      max.depth = 5L,
                                      type = c("auto", "regression", "classification"),
                                      ml.options = ml_options(),
                                      ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    df,
    response,
    features,
    NULL,
    environment(),
    categorical.transformations
  )

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  only.model <- ensure_scalar_boolean(ml.options$only.model)
  type <- match.arg(type)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- sdf_schema(df)
  responseType <- schema[[response]]$type

  regressor  <- "org.apache.spark.ml.regression.GBTRegressor"
  classifier <- "org.apache.spark.ml.classification.GBTClassifier"

  envir$model <- if (identical(type, "regression"))
    regressor
  else if (identical(type, "classification"))
    classifier
  else if (responseType %in% c("DoubleType", "IntegerType"))
    regressor
  else
    classifier

  rf <- invoke_new(sc, envir$model)

  model <- rf %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

  fit <- model %>%
    invoke("fit", tdf)

  ml_model("gradient_boosted_trees", fit,
    features = features,
    response = response,
    max.bins = max.bins,
    max.depth = max.depth,
    trees = invoke(fit, "trees"),
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
