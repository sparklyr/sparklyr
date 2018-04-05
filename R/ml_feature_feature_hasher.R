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
  x, input_cols, output_col,
  num_features = as.integer(2^18),
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"), ...
  ) {
  UseMethod("ft_feature_hasher")
}

#' @export
ft_feature_hasher.spark_connection <- function(
  x, input_cols, output_col,
  num_features = as.integer(2^18),
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"), ...
  ) {

  ml_ratify_args()
  jobj <- invoke_new(x, "org.apache.spark.ml.feature.FeatureHasher", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCol", output_col) %>%
    invoke("setNumFeatures", num_features)

  if (!is.null(categorical_cols))
    jobj <- invoke("setCategoricalCols", categorical_cols)

  new_ml_feature_hasher(jobj)
}

#' @export
ft_feature_hasher.ml_pipeline <- function(
  x, input_cols, output_col,
  num_features = as.integer(2^18),
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_feature_hasher.tbl_spark <- function(
  x, input_cols, output_col,
  num_features = as.integer(2^18),
  categorical_cols = NULL,
  uid = random_string("feature_hasher_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_feature_hasher <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_feature_hasher")
}

ml_validator_feature_hasher <- function(args, nms) {
  args %>%
    ml_validate_args({
      input_cols <- lapply(input_cols, ensure_scalar_character)
      output_col <- ensure_scalar_character(output_col)
      if (!is.null(categorical_cols))
        categorical_cols <- lapply(categorical_cols, ensure_scalar_character)
      num_features <- ensure_scalar_integer(num_features)
      uid <- ensure_scalar_character(uid)
    }) %>%
    ml_extract_args(nms)
}
