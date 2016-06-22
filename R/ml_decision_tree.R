#' Spark ML -- Decision Trees
#'
#' Perform regression or classification using decision trees.
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
#'
#' @family Spark ML routines
#'
#' @export
ml_decision_tree <- function(x,
                             response,
                             features,
                             max.bins = 32L,
                             max.depth = 5L,
                             type = c("auto", "regression", "classification"))
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  type <- match.arg(type)
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  # choose classification vs. regression model based on column type
  schema <- spark_dataframe_schema(df)
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
  
  rf <- sparkapi_invoke_new(sc, model)
  
  fit <- rf %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setMaxBins", max.bins) %>%
    sparkapi_invoke("setMaxDepth", max.depth) %>%
    sparkapi_invoke("fit", tdf)
  
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
  cat(sparkapi_invoke(x$.model, "toString"), sep = "\n")
}
