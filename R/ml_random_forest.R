spark_ml_random_forest <- function(x, response, features,
                                   max.bins, max.depth, num.trees)
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- spark_dataframe_schema(df)
  responseType <- schema[[response]]$type
  model <- if (responseType %in% c("DoubleType", "IntegerType"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else
    "org.apache.spark.ml.classification.RandomForestClassifier"
  rf <- spark_invoke_static_ctor(scon, model)

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
#'
#' @family Spark ML routines
#'
#' @export
ml_random_forest <- function(x, response, features,
                             max.bins = 32L, max.depth = 5L, num.trees = 20L)
{
  spark_ml_random_forest(x, response, features, max.bins, max.depth, num.trees)
}

#' @export
print.ml_model_random_forest <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(spark_invoke(x$.model, "toString"), sep = "\n")
}
