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
ft_chisq_selector <- function(
  x,
  features_col = "features",
  output_col = NULL,
  label_col = "label",
  selector_type = "numTopFeatures",
  fdr = 0.05,
  fpr = 0.05,
  fwe = 0.05,
  num_top_features = 50,
  percentile = 0.1,
  uid = random_string("chisq_selector_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_chisq_selector")
}

ml_chisq_selector <- ft_chisq_selector

#' @export
ft_chisq_selector.spark_connection <- function(
  x,
  features_col = "features",
  output_col = NULL,
  label_col = "label",
  selector_type = "numTopFeatures",
  fdr = 0.05,
  fpr = 0.05,
  fwe = 0.05,
  num_top_features = 50,
  percentile = 0.1,
  uid = random_string("chisq_selector_"),
  ...
) {
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
    x,
    "org.apache.spark.ml.feature.ChiSqSelector",
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    (function(obj) {
      do.call(
        invoke,
        c(
          obj,
          "%>%",
          Filter(
            function(x) !is.null(x),
            list(
              jobj_set_param_helper(
                obj,
                "setFdr",
                .args[["fdr"]],
                "2.2.0",
                0.05
              ),
              list("setFeaturesCol", .args[["features_col"]]),
              jobj_set_param_helper(
                obj,
                "setFpr",
                .args[["fpr"]],
                "2.1.0",
                0.05
              ),
              jobj_set_param_helper(
                obj,
                "setFwe",
                .args[["fwe"]],
                "2.2.0",
                0.05
              ),
              list("setLabelCol", .args[["label_col"]]),
              list("setNumTopFeatures", .args[["num_top_features"]]),
              jobj_set_param_helper(
                obj,
                "setPercentile",
                .args[["percentile"]],
                "2.1.0",
                0.1
              ),
              jobj_set_param_helper(
                obj,
                "setSelectorType",
                .args[["selector_type"]],
                "2.1.0",
                "numTopFeatures"
              )
            )
          )
        )
      )
    }) %>%
    new_ml_chisq_selector()

  estimator
}

#' @export
ft_chisq_selector.ml_pipeline <- function(
  x,
  features_col = "features",
  output_col = NULL,
  label_col = "label",
  selector_type = "numTopFeatures",
  fdr = 0.05,
  fpr = 0.05,
  fwe = 0.05,
  num_top_features = 50,
  percentile = 0.1,
  uid = random_string("chisq_selector_"),
  ...
) {
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
ft_chisq_selector.tbl_spark <- function(
  x,
  features_col = "features",
  output_col = NULL,
  label_col = "label",
  selector_type = "numTopFeatures",
  fdr = 0.05,
  fpr = 0.05,
  fwe = 0.05,
  num_top_features = 50,
  percentile = 0.1,
  uid = random_string("chisq_selector_"),
  ...
) {
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
  .args[["num_top_features"]] <- cast_scalar_integer(.args[[
    "num_top_features"
  ]])
  .args[["percentile"]] <- cast_scalar_double(.args[["percentile"]])
  .args[["selector_type"]] <- cast_choice(
    .args[["selector_type"]],
    c("numTopFeatures", "percentile", "fpr", "fdr", "fwe")
  )
  .args[["uid"]] <- cast_string(.args[["uid"]])
  .args
}

#' Feature Transformation -- FeatureHasher (Transformer)
#'
#' @details Feature hashing projects a set of categorical or numerical features into a
#'   feature vector of specified dimension (typically substantially smaller than
#'   that of the original feature space). This is done using the hashing trick
#'   \url{https://en.wikipedia.org/wiki/Feature_hashing} to map features to indices
#'   in the feature vector.
#'
#'   The FeatureHasher transformer operates on multiple columns. Each column may
#'     contain either numeric or categorical features. Behavior and handling of
#'     column data types is as follows: -Numeric columns: For numeric features,
#'     the hash value of the column name is used to map the feature value to its
#'     index in the feature vector. By default, numeric features are not treated
#'     as categorical (even when they are integers). To treat them as categorical,
#'     specify the relevant columns in categoricalCols. -String columns: For
#'      categorical features, the hash value of the string "column_name=value"
#'      is used to map to the vector index, with an indicator value of 1.0.
#'      Thus, categorical features are "one-hot" encoded (similarly to using
#'      OneHotEncoder with drop_last=FALSE). -Boolean columns: Boolean values
#'      are treated in the same way as string columns. That is, boolean features
#'      are represented as "column_name=true" or "column_name=false", with an
#'      indicator value of 1.0.
#'
#'  Null (missing) values are ignored (implicitly zero in the resulting feature vector).
#'
#'  The hash function used here is also the MurmurHash 3 used in HashingTF. Since a
#'  simple modulo on the hashed value is used to determine the vector index, it is
#'  advisable to use a power of two as the num_features parameter; otherwise the
#'  features will not be mapped evenly to the vector indices.
#'
#' @param input_cols Names of input columns.
#' @param output_col Name of output column.
#' @param num_features Number of features. Defaults to \eqn{2^18}.
#' @param categorical_cols Numeric columns to treat as categorical features.
#'   By default only string and boolean columns are treated as categorical,
#'   so this param can be used to explicitly specify the numerical columns to
#'   treat as categorical.
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_feature_hasher <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  num_features = 2^18,
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_feature_hasher")
}

ml_feature_hasher <- ft_feature_hasher

#' @export
ft_feature_hasher.spark_connection <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  num_features = 2^18,
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"),
  ...
) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    num_features = num_features,
    categorical_cols = categorical_cols,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_feature_hasher()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.FeatureHasher",
    input_cols = .args[["input_cols"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setNumFeatures", .args[["num_features"]]) %>%
    jobj_set_param("setCategoricalCols", .args[["categorical_cols"]])

  new_ml_feature_hasher(jobj)
}

#' @export
ft_feature_hasher.ml_pipeline <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  num_features = 2^18,
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"),
  ...
) {
  stage <- ft_feature_hasher.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    num_features = num_features,
    categorical_cols = categorical_cols,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_feature_hasher.tbl_spark <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  num_features = 2^18,
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"),
  ...
) {
  stage <- ft_feature_hasher.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    num_features = num_features,
    categorical_cols = categorical_cols,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_feature_hasher <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_feature_hasher")
}

validator_ml_feature_hasher <- function(.args) {
  .args[["input_cols"]] <- cast_string_list(
    .args[["input_cols"]],
    allow_null = TRUE
  )
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args[["categorical_cols"]] <- cast_string_list(
    .args[["categorical_cols"]],
    allow_null = TRUE
  )
  .args[["num_features"]] <- cast_scalar_integer(.args[["num_features"]])
  .args[["uid"]] <- cast_string(.args[["uid"]])
  .args
}

#' Feature Transformation -- QuantileDiscretizer (Estimator)
#'
#' \code{ft_quantile_discretizer} takes a column with continuous features and outputs
#'   a column with binned categorical features. The number of bins can be
#'   set using the \code{num_buckets} parameter. It is possible that the number
#'   of buckets used will be smaller than this value, for example, if there
#'   are too few distinct values of the input to create enough distinct
#'   quantiles.
#'
#'   NaN handling: null and NaN values will be ignored from the column
#'   during \code{QuantileDiscretizer} fitting. This will produce a \code{Bucketizer}
#'   model for making predictions. During the transformation, \code{Bucketizer}
#'   will raise an error when it finds NaN values in the dataset, but the
#'   user can also choose to either keep or remove NaN values within the
#'   dataset by setting \code{handle_invalid} If the user chooses to keep NaN values,
#'   they will be handled specially and placed into their own bucket,
#'   for example, if 4 buckets are used, then non-NaN data will be put
#'   into buckets[0-3], but NaNs will be counted in a special bucket[4].
#'
#'   Algorithm: The bin ranges are chosen using an approximate algorithm (see
#'   the documentation for org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions}{here} for a detailed description). The precision of the approximation can be
#'   controlled with the \code{relative_error} parameter. The lower and upper bin
#'   bounds will be -Infinity and +Infinity, covering all real values.
#'
#'   Note that the result may be different every time you run it, since the sample
#'   strategy behind it is non-deterministic.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#'
#' @param input_cols Names of input columns.
#' @param output_cols Names of output columns.
#' @param num_buckets Number of buckets (quantiles, or categories) into which data
#'   points are grouped. Must be greater than or equal to 2.
#' @param num_buckets_array Array of number of buckets (quantiles, or categories)
#'   into which data points are grouped. Each value must be greater than or equal to 2.
#' @param relative_error (Spark 2.0.0+) Relative error (see documentation for
#'   org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions}{here}
#'   for description). Must be in the range [0, 1]. default: 0.001
#' @param weight_column If not NULL, then a generalized version of the Greenwald-Khanna algorithm will be run to compute
#'   weighted percentiles, with each input having a relative weight specified by the corresponding value in `weight_column`.
#'   The weights can be considered as relative frequencies of sample inputs.
#'
#' @seealso \code{\link{ft_bucketizer}}
#' @export
ft_quantile_discretizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_buckets = 2,
  input_cols = NULL,
  output_cols = NULL,
  num_buckets_array = NULL,
  handle_invalid = "error",
  relative_error = 0.001,
  uid = random_string("quantile_discretizer_"),
  weight_column = NULL,
  ...
) {
  check_dots_used()
  UseMethod("ft_quantile_discretizer")
}

ml_quantile_discretizer <- ft_quantile_discretizer

#' @export
ft_quantile_discretizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_buckets = 2,
  input_cols = NULL,
  output_cols = NULL,
  num_buckets_array = NULL,
  handle_invalid = "error",
  relative_error = 0.001,
  uid = random_string("quantile_discretizer_"),
  weight_column = NULL,
  ...
) {
  if (!is.null(weight_column) && spark_version(x) < "3.0.0") {
    stop(
      "Weighted quantile discretizer is only supported in Spark 3.0 or above."
    )
  }

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    uid = uid,
    weight_column = weight_column
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_quantile_discretizer()

  jobj <- spark_pipeline_stage(
    x,
    if (is.null(weight_column)) {
      "org.apache.spark.ml.feature.QuantileDiscretizer"
    } else {
      "org.apache.spark.ml.feature.WeightedQuantileDiscretizer"
    },
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    input_cols = .args[["input_cols"]],
    output_cols = .args[["output_cols"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param(
      "setHandleInvalid",
      .args[["handle_invalid"]],
      "2.1.0",
      "error"
    ) %>%
    jobj_set_param(
      "setRelativeError",
      .args[["relative_error"]],
      "2.0.0",
      0.001
    )
  if (!is.null(weight_column)) {
    jobj <- jobj %>%
      jobj_set_param("setWeightCol", .args[["weight_column"]], "2.3.0", NULL)
  }

  if (!is.null(input_col) && !is.null(output_col)) {
    jobj <- jobj %>%
      jobj_set_param("setNumBuckets", .args[["num_buckets"]])
  } else if (!is.null(input_cols) && !is.null(output_cols)) {
    jobj <- jobj %>%
      jobj_set_param(
        "setNumBucketsArray",
        .args[["num_buckets_array"]],
        "2.3.0"
      )
  }

  estimator <- jobj %>%
    new_ml_quantile_discretizer()

  estimator
}

#' @export
ft_quantile_discretizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_buckets = 2,
  input_cols = NULL,
  output_cols = NULL,
  num_buckets_array = NULL,
  handle_invalid = "error",
  relative_error = 0.001,
  uid = random_string("quantile_discretizer_"),
  ...
) {
  stage <- ft_quantile_discretizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_quantile_discretizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_buckets = 2,
  input_cols = NULL,
  output_cols = NULL,
  num_buckets_array = NULL,
  handle_invalid = "error",
  relative_error = 0.001,
  uid = random_string("quantile_discretizer_"),
  ...
) {
  stage <- ft_quantile_discretizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_quantile_discretizer <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_quantile_discretizer")
}

validator_ml_quantile_discretizer <- function(.args) {
  .args[["uid"]] <- cast_string(.args[["uid"]])

  if (!is.null(.args[["input_col"]]) && !is.null(.args[["input_cols"]])) {
    stop(
      "Only one of `input_col` or `input_cols` may be specified.",
      call. = FALSE
    )
  }
  .args[["input_col"]] <- cast_nullable_string(.args[["input_col"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args[["num_buckets"]] <- cast_scalar_integer(.args[["num_buckets"]])
  .args[["input_cols"]] <- cast_string_list(
    .args[["input_cols"]],
    allow_null = TRUE
  )
  .args[["output_cols"]] <- cast_string_list(
    .args[["output_cols"]],
    allow_null = TRUE
  )
  .args[["num_buckets_array"]] <- cast_integer_list(
    .args[["num_buckets_array"]],
    allow_null = TRUE
  )
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args[["relative_error"]] <- cast_scalar_double(.args[["relative_error"]])
  .args
}
