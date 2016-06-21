#' Spark ML -- Random Forests
#'
#' Perform regression or classification using random forests with a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) included in the model.
#' @param max.bins The maximum number of bins used for discretizing
#'   continuous features and for choosing how to split on features at
#'   each node. More bins give higher granularity.
#' @param max.depth Maximum depth of the tree (>= 0); that is, the maximum
#'   number of nodes separating any leaves from the root of the tree.
#' @param num.trees Number of trees to train (>= 1).
#' @param type The type of model to fit. \code{"regression"} treats the response
#'   as a continuous variable, while \code{"classification"} treats the response
#'   as a categorical variable. When \code{"auto"} is used, the model type is
#'   inferred based on the response variable type -- if it is a numeric type,
#'   then regression is used; classification otherwise.
#'
#' @family Spark ML routines
#'
#' @export
ml_random_forest <- function(x,
                             response,
                             features,
                             max.bins = 32L,
                             max.depth = 5L,
                             num.trees = 20L,
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
  model <- if (identical(type, "regression"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else if (identical(type, "classification"))
    "org.apache.spark.ml.classification.RandomForestClassifier"
  else if (responseType %in% c("DoubleType", "IntegerType"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else
    "org.apache.spark.ml.classification.RandomForestClassifier"

  rf <- spark_invoke_new(sc, model)

  fit <- rf %>%
    spark_invoke("setFeaturesCol", envir$features) %>%
    spark_invoke("setLabelCol", envir$response) %>%
    spark_invoke("setMaxBins", max.bins) %>%
    spark_invoke("setMaxDepth", max.depth) %>%
    spark_invoke("setNumTrees", num.trees) %>%
    spark_invoke("fit", tdf)

  featureImportances <- fit %>%
    spark_invoke("featureImportances") %>%
    spark_invoke("toArray")

  ml_model("random_forest", fit,
           features = features,
           response = response,
           max.bins = max.bins,
           max.depth = max.depth,
           num.trees = num.trees,
           feature.importances = featureImportances,
           trees = spark_invoke(fit, "trees"),
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_random_forest <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(spark_invoke(x$.model, "toString"), sep = "\n")
}
