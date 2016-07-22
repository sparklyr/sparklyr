#' Spark ML -- Random Forests
#'
#' Perform regression or classification using random forests with a \code{spark_tbl}.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-decision-trees-max-bins
#' @template roxlate-ml-decision-trees-max-depth
#' @template roxlate-ml-decision-trees-num-trees
#' @template roxlate-ml-decision-trees-type
#' @template roxlate-ml-dots
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
                             type = c("auto", "regression", "classification"),
                             ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  df <- ml_prepare_response_features_intercept(df, response, features, NULL)

  max.bins <- ensure_scalar_integer(max.bins)
  max.depth <- ensure_scalar_integer(max.depth)
  num.trees <- ensure_scalar_integer(num.trees)
  type <- match.arg(type)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  # choose classification vs. regression model based on column type
  schema <- sdf_schema(df)
  responseType <- schema[[response]]$type
  model <- if (identical(type, "regression"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else if (identical(type, "classification"))
    "org.apache.spark.ml.classification.RandomForestClassifier"
  else if (responseType %in% c("DoubleType", "IntegerType"))
    "org.apache.spark.ml.regression.RandomForestRegressor"
  else
    "org.apache.spark.ml.classification.RandomForestClassifier"

  rf <- invoke_new(sc, model)

  model <- rf %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setMaxBins", max.bins) %>%
    invoke("setMaxDepth", max.depth) %>%
    invoke("setNumTrees", num.trees)

  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

  featureImportances <- fit %>%
    invoke("featureImportances") %>%
    invoke("toArray")

  ml_model("random_forest", fit,
           features = features,
           response = response,
           max.bins = max.bins,
           max.depth = max.depth,
           num.trees = num.trees,
           feature.importances = featureImportances,
           trees = invoke(fit, "trees"),
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_random_forest <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
