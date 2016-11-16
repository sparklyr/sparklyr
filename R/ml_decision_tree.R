#' Spark ML -- Decision Trees
#'
#' Perform regression or classification using decision trees.
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
ml_decision_tree <- function(x,
                             response,
                             features,
                             max.bins = 32L,
                             max.depth = 5L,
                             type = c("auto", "regression", "classification"),
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
  type <- match.arg(type)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- sdf_schema(df)
  responseType <- schema[[response]]$type

  regressor  <- "org.apache.spark.ml.regression.DecisionTreeRegressor"
  classifier <- "org.apache.spark.ml.classification.DecisionTreeClassifier"

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

  ml_model("decision_tree", fit,
           features = features,
           response = response,
           max.bins = max.bins,
           max.depth = max.depth,
           categorical.transformations = categorical.transformations,
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
