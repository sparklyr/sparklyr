#' Feature Transformation -- ChiSqSelector (Estimator)
#'
#' Chi-Squared feature selection, which selects categorical features to use for predicting a categorical label
#'
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param fdr (Spark 2.2.0+) The upper bound of the expected false discovery rate. Only applicable when selector_type = "fdr". Default value is 0.05.
#' @template roxlate-ml-features-col
#' @param fpr (Spark 2.1.0+) The highest p-value for features to be kept. Only applicable when selector_type= "fpr". Default value is 0.05.
#' @param fwe (Spark 2.2.0+) The upper bound of the expected family-wise error rate. Only applicable when selector_type = "fwe". Default value is 0.05.
#' @template roxlate-ml-label-col
#' @param num_top_features Number of features that selector will select, ordered by ascending p-value. If the number of features is less than \code{num_top_features}, then this will select all features. Only applicable when selector_type = "numTopFeatures". The default value of \code{num_top_features} is 50.
#' @param percentile (Spark 2.1.0+) Percentile of features that selector will select, ordered by statistics value descending. Only applicable when selector_type = "percentile". Default value is 0.1.
#' @param selector_type (Spark 2.1.0+) The selector type of the ChisqSelector. Supported options: "numTopFeatures" (default), "percentile", "fpr", "fdr", "fwe".
#'
#'
#' @export
ft_chisq_selector <- function(x, features_col = "features", output_col = NULL, label_col = "label",
                              selector_type = "numTopFeatures", fdr = 0.05, fpr = 0.05, fwe = 0.05,
                              num_top_features = 50, percentile = 0.1,
                              uid = random_string("chisq_selector_"), ...) {
  check_dots_used()
  UseMethod("ft_chisq_selector")
}

ml_chisq_selector <- ft_chisq_selector

#' @export
ft_chisq_selector.spark_connection <- function(x, features_col = "features", output_col = NULL, label_col = "label",
                                               selector_type = "numTopFeatures", fdr = 0.05, fpr = 0.05, fwe = 0.05,
                                               num_top_features = 50, percentile = 0.1,
                                               uid = random_string("chisq_selector_"), ...) {
  .args <- list(
    features_col = features_col,
    output_col = output_col,
    label_col = label_col,
    selector_type = selector_type,
    fdr = fdr,
    fpr = fpr,
    fwe = fwe,
    num_top_features = num_top_features,
    percentile = percentile,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_chisq_selector()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.ChiSqSelector",
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    (
      function(obj) {
        do.call(
          invoke,
          c(obj, "%>%", Filter(
            function(x) !is.null(x),
            list(
              jobj_set_param_helper(obj, "setFdr", .args[["fdr"]], "2.2.0", 0.05),
              list("setFeaturesCol", .args[["features_col"]]),
              jobj_set_param_helper(obj, "setFpr", .args[["fpr"]], "2.1.0", 0.05),
              jobj_set_param_helper(obj, "setFwe", .args[["fwe"]], "2.2.0", 0.05),
              list("setLabelCol", .args[["label_col"]]),
              list("setNumTopFeatures", .args[["num_top_features"]]),
              jobj_set_param_helper(obj, "setPercentile", .args[["percentile"]], "2.1.0", 0.1),
              jobj_set_param_helper(obj, "setSelectorType", .args[["selector_type"]], "2.1.0", "numTopFeatures")
            )
          ))
        )
      }) %>%
    new_ml_chisq_selector()

  estimator
}

#' @export
ft_chisq_selector.ml_pipeline <- function(x, features_col = "features", output_col = NULL, label_col = "label",
                                          selector_type = "numTopFeatures", fdr = 0.05, fpr = 0.05, fwe = 0.05,
                                          num_top_features = 50, percentile = 0.1,
                                          uid = random_string("chisq_selector_"), ...) {
  stage <- ft_chisq_selector.spark_connection(
    x = spark_connection(x),
    features_col = features_col,
    output_col = output_col,
    label_col = label_col,
    selector_type = selector_type,
    fdr = fdr,
    fpr = fpr,
    fwe = fwe,
    num_top_features = num_top_features,
    percentile = percentile,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_chisq_selector.tbl_spark <- function(x, features_col = "features", output_col = NULL, label_col = "label",
                                        selector_type = "numTopFeatures", fdr = 0.05, fpr = 0.05, fwe = 0.05,
                                        num_top_features = 50, percentile = 0.1,
                                        uid = random_string("chisq_selector_"), ...) {
  stage <- ft_chisq_selector.spark_connection(
    x = spark_connection(x),
    features_col = features_col,
    output_col = output_col,
    label_col = label_col,
    selector_type = selector_type,
    fdr = fdr,
    fpr = fpr,
    fwe = fwe,
    num_top_features = num_top_features,
    percentile = percentile,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_chisq_selector <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_chisq_selector")
}

new_ml_chisq_selector_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_chisq_selector_model")
}

validator_ml_chisq_selector <- function(.args) {
  .args[["features_col"]] <- cast_string(.args[["features_col"]])
  .args[["label_col"]] <- cast_string(.args[["label_col"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args[["fdr"]] <- cast_scalar_double(.args[["fdr"]])
  .args[["fpr"]] <- cast_scalar_double(.args[["fpr"]])
  .args[["fwe"]] <- cast_scalar_double(.args[["fwe"]])
  .args[["num_top_features"]] <- cast_scalar_integer(.args[["num_top_features"]])
  .args[["percentile"]] <- cast_scalar_double(.args[["percentile"]])
  .args[["selector_type"]] <- cast_choice(
    .args[["selector_type"]],
    c("numTopFeatures", "percentile", "fpr", "fdr", "fwe")
  )
  .args[["uid"]] <- cast_string(.args[["uid"]])
  .args
}
