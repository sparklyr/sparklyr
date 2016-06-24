#' Spark ML -- Gradient-Boosted Tree
#'
#' Perform regression or classification using gradient-boosted trees.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) included in the model.
#' @param max.bins The maximum number of bins used for discretizing
#'   continuous features and for choosing how to split on features at
#'   each node. More bins give higher granularity.
#' @param max.depth Maximum depth of the tree (>= 0); that is, the maximum
#'   number of nodes separating any leaves from the root of the tree.
#' @param type The type of model to fit. \code{"regression"} treats the response
#'   as a continuous variable, while \code{"classification"} treats the response
#'   as a categorical variable. When \code{"auto"} is used, the model type is
#'   inferred based on the response variable type -- if it is a numeric type,
#'   then regression is used; classification otherwise.
#' @param ... Other arguments passed on to methods.
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
                                      ...)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  type <- match.arg(type)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  # choose classification vs. regression model based on column type
  schema <- spark_dataframe_schema(df)
  responseType <- schema[[response]]$type

  regressor  <- "org.apache.spark.ml.regression.GBTRegressor"
  classifier <- "org.apache.spark.ml.classification.GBTClassifier"

  model <- if (identical(type, "regression"))
    regressor
  else if (identical(type, "classification"))
    classifier
  else if (responseType %in% c("DoubleType", "IntegerType"))
    regressor
  else
    classifier

  rf <- sparkapi_invoke_new(sc, model)

  model <- rf %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setMaxBins", max.bins) %>%
    sparkapi_invoke("setMaxDepth", max.depth)
  
  if (only_model) return(model)
  
  fit <- model %>%
    sparkapi_invoke("fit", tdf)

  ml_model("gradient_boosted_trees", fit,
    features = features,
    response = response,
    max.bins = max.bins,
    max.depth = max.depth,
    trees = sparkapi_invoke(fit, "trees"),
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_gradient_boosted_trees <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(sparkapi_invoke(x$.model, "toString"), sep = "\n")
}
