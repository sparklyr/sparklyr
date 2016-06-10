spark_ml_random_forest <- function(x, response, features, max.bins, max.depth, num.trees)
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)
  schema <- spark_dataframe_schema(x)

  # If the response variable is categorical, treat this
  # as classification; otherwise, use regression.
  responseType <- schema[[response]]$type
  model <- if (responseType %in% c("DoubleType"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else
    "org.apache.spark.ml.classification.RandomForestClassifier"

  rf <- spark_invoke_static_ctor(scon, model)
  tdf <- spark_assemble_vector(scon, df, features, "features")

  rf %>%
    spark_invoke("setLabelCol", response) %>%
    spark_invoke("setMaxBins", max.bins) %>%
    spark_invoke("setMaxDepth", max.depth) %>%
    spark_invoke("setNumTrees", num.trees) %>%
    spark_invoke("setFeaturesCol", "features") %>%
    spark_invoke("fit", tdf)
}

as_random_forest_result <- function(fit, response, features)
{
  ml_model("random_forest", fit,
    response = response,
    features = features
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
  fit <- spark_ml_random_forest(x, response, features,
                                max.bins, max.depth, num.trees)
  as_random_forest_result(fit, features, response)
}

#' @export
print.ml_model_random_forest <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(spark_invoke(x$.model, "toString"), sep = "\n")
}

#' @export
predict.ml_model_random_forest <- function(object, newdata, ...) {
  sdf <- as_spark_dataframe(newdata)
  assembled <- spark_assemble_vector(sdf$scon, sdf, features(object), "features")
  predicted <- spark_invoke(object$.model, "transform", assembled)
  spark_dataframe_read_column(predicted, "prediction")
}
