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
                             ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  prepare_response_features_intercept(df, response, features, NULL)

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  type <- match.arg(type)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- sdf_schema(df)
  responseType <- schema[[response]]$type

  regressor  <- "org.apache.spark.ml.regression.DecisionTreeRegressor"
  classifier <- "org.apache.spark.ml.classification.DecisionTreeClassifier"

  model <- if (identical(type, "regression"))
    regressor
  else if (identical(type, "classification"))
    classifier
  else if (responseType %in% c("DoubleType", "IntegerType"))
    regressor
  else
    classifier

  rf <- invoke_new(sc, model)

  model <- rf %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth)

  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

  ml_model("decision_tree", fit,
           features = features,
           response = response,
           max.bins = max.bins,
           max.depth = max.depth,
           model.parameters = as.list(envir)
  )
}

#' @export
print.decision_tree <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
