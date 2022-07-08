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
ft_chisq_selector <- function(x, features_col = "features", output_col = NULL,
                              label_col = "label", selector_type = "numTopFeatures",
                              fdr = 0.05, fpr = 0.05, fwe = 0.05,
                              num_top_features = 50, percentile = 0.1,
                              uid = random_string("chisq_selector_"), ...) {
  check_dots_used()
  UseMethod("ft_chisq_selector")
}

ml_chisq_selector <- ft_chisq_selector


ft_chisq_selector_impl <- function(x, features_col = "features", output_col = NULL,
                                   label_col = "label", selector_type = "numTopFeatures",
                                   fdr = 0.05, fpr = 0.05, fwe = 0.05,
                                   num_top_features = 50, percentile = 0.1,
                                   uid = random_string("chisq_selector_"), ...) {

  fdr <- param_min_version(x, fdr, "2.2.0")
  fpr <- param_min_version(x, fpr, "2.1.0")
  percentile <- param_min_version(x, percentile, "2.1.0")
  selector_type <- param_min_version(x, selector_type, "2.1.0")
  fwe <- param_min_version(x, fwe, "2.2.0")

  est_process_model(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.ChiSqSelector",
    r_class = "ml_chisq_selector",
    invoke_steps = list(
      setFeaturesCol = cast_string(features_col),
      setOutputCol = cast_nullable_string(output_col),
      setLabelCol = cast_string(label_col),
      setSelectorType = cast_choice(
        selector_type,
        c("numTopFeatures", "percentile", "fpr", "fdr", "fwe")
      ),
      setFdr = cast_scalar_double(fdr),
      setFpr = cast_scalar_double(fpr),
      setFwe = cast_scalar_double(fwe),
      setNumTopFeatures = cast_scalar_integer(num_top_features),
      setPercentile = cast_scalar_double(percentile)
    )
  )
}

#' @export
ft_chisq_selector.spark_connection <- ft_chisq_selector_impl

#' @export
ft_chisq_selector.ml_pipeline <- ft_chisq_selector_impl

#' @export
ft_chisq_selector.tbl_spark <- ft_chisq_selector_impl

new_ml_chisq_selector_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_chisq_selector_model")
}
