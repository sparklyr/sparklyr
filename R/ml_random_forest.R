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
    max.bins = spark_invoke(fit, "getMaxBins"),
    max.depth = spark_invoke(fit, "getMaxDepth"),
    num.trees = spark_invoke(fit, "getNumTrees"),
    feature.importances = featureImportances,
    trees = spark_invoke(fit, "trees"),
    model.parameters = as.list(envir)
  )
}

#' Random Forests with Spark ML
#'
#' @param x A dplyr source.
#' @param response The response variable -- if the response is a factor or a logical
#'   vector, then classification is performed (with the column treated as a label);
#'   otherwise, regression is performed.
#' @param features List of columns to use as features.
#' @param max.bins Maximum number of bins.
#' @param max.depth Maximum depth.
#' @param num.trees Maximum number of trees.
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
