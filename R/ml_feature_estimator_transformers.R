#' Feature Tranformation -- RFormula (Estimator)
#'
#' Implements the transforms required for fitting a dataset against an R model
#'   formula. Currently we support a limited subset of the R operators,
#'   including \code{~}, \code{.}, \code{:}, \code{+}, and \code{-}. Also see the R formula docs here:
#'   \url{http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html}
#'
#' @details The basic operators in the formula are:
#'
#'   \itemize{
#'     \item ~ separate target and terms
#'     \item + concat terms, "+ 0" means removing intercept
#'     \item - remove a term, "- 1" means removing intercept
#'     \item : interaction (multiplication for numeric values, or binarized categorical values)
#'     \item . all columns except target
#'   }
#'
#'   Suppose a and b are double columns, we use the following simple examples to illustrate the
#'   effect of RFormula:
#'
#'   \itemize{
#'     \item \code{y ~ a + b} means model \code{y ~ w0 + w1 * a + w2 * b}
#'       where \code{w0} is the intercept and \code{w1, w2} are coefficients.
#'     \item \code{y ~ a + b + a:b - 1} means model \code{y ~ w1 * a + w2 * b + w3 * a * b}
#'       where \code{w1, w2, w3} are coefficients.
#'   }
#'
#'  RFormula produces a vector column of features and a double or string column
#'  of label. Like when formulas are used in R for linear regression, string
#'  input columns will be one-hot encoded, and numeric columns will be cast to
#'  doubles. If the label column is of type string, it will be first transformed
#'  to double with StringIndexer. If the label column does not exist in the
#'  DataFrame, the output label column will be created from the specified
#'  response variable in the formula.
#'
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#'
#' @param formula R formula as a character string or a formula. Formula objects are
#'   converted to character strings directly and the environment is not captured.
#' @param force_index_label Force to index label whether it is numeric or
#'   string type. Usually we index label only when it is string type. If
#'   the formula was used by classification algorithms, we can force to index
#'   label even it is numeric type by setting this param with true.
#'   Default: \code{FALSE}.
#'
#' @export
ft_r_formula <- function(x, formula, features_col = "features", label_col = "label",
                         force_index_label = FALSE, dataset = NULL,
                         uid = random_string("r_formula_"), ...) {
  UseMethod("ft_r_formula")
}

#' @export
ft_r_formula.spark_connection <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...) {

  estimator <- invoke_new(x, "org.apache.spark.ml.feature.RFormula", uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setForceIndexLabel", force_index_label) %>%
    invoke("setFormula", formula) %>%
    invoke("setLabelCol", label_col) %>%
    new_ml_estimator()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_r_formula.ml_pipeline <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_r_formula.tbl_spark <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

# StringIndexer

#' Feature Tranformation -- StringIndexer (Estimator)
#'
#' A label indexer that maps a string column of labels to an ML column of
#'   label indices. If the input column is numeric, we cast it to string and
#'   index the string values. The indices are in \code{[0, numLabels)}, ordered by
#'   label frequencies. So the most frequent label gets index 0. This function
#'   is the inverse of \code{\link{ft_index_to_string}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#' @seealso \code{\link{ft_index_to_string}}
#' @export
ft_string_indexer <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...) {
  UseMethod("ft_string_indexer")
}

#' @export
ft_string_indexer.spark_connection <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...) {

  ml_validate_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.StringIndexer",
                                  input_col, output_col, uid) %>%
    invoke("setHandleInvalid", handle_invalid) %>%
    new_ml_estimator()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_string_indexer.ml_pipeline <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_string_indexer.tbl_spark <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  # backwards compatibility for params argument
  if (rlang::has_name(dots, "params") && rlang::is_env(dots$params)) {
    transformer <- if (is_ml_transformer(stage))
      stage
    else
      ml_fit(stage, x)
    dots$params$labels <- transformer$.jobj %>%
      invoke("labels") %>%
      as.character()
    transformer %>%
      ml_transform(x)
  } else {
    if (is_ml_transformer(stage))
      ml_transform(stage, x)
    else
      ml_fit_and_transform(stage, x)
  }
}

#' Feature Tranformation -- CountVectorizer (Estimator)
#'
#' Extracts a vocabulary from document collections.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#'
#' @param binary Binary toggle to control the output vector values.
#'   If \code{TRUE}, all nonzero counts (after \code{min_tf} filter applied)
#'   are set to 1. This is useful for discrete probabilistic models that
#'    model binary events rather than integer counts. Default: \code{FALSE}
#' @param min_df Specifies the minimum number of different documents a
#'   term must appear in to be included in the vocabulary. If this is an
#'   integer greater than or equal to 1, this specifies the number of
#'   documents the term must appear in; if this is a double in [0,1), then
#'   this specifies the fraction of documents. Default: 1.
#' @param min_tf Filter to ignore rare words in a document. For each
#'   document, terms with frequency/count less than the given threshold
#'   are ignored. If this is an integer greater than or equal to 1, then
#'   this specifies a count (of times the term must appear in the document);
#'   if this is a double in [0,1), then this specifies a fraction (out of
#'   the document's token count). Default: 1.
#' @param vocab_size Build a vocabulary that only considers the top
#'   \code{vocab_size} terms ordered by term frequency across the corpus.
#'   Default: \code{2^18}.
#'
#' @export
ft_count_vectorizer <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...) {
  UseMethod("ft_count_vectorizer")
}

#' @export
ft_count_vectorizer.spark_connection <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...) {

  ml_validate_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.CountVectorizer",
                                  input_col, output_col, uid) %>%
    invoke("setBinary", binary) %>%
    invoke("setMinDF", min_df) %>%
    invoke("setMinTF", min_tf) %>%
    invoke("setVocabSize", vocab_size) %>%
    new_ml_count_vectorizer()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_count_vectorizer.ml_pipeline <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_count_vectorizer.tbl_spark <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
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
#'   \link[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions]{here} for a detailed description). The precision of the approximation can be
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
#' @param num_buckets Number of buckets (quantiles, or categories) into which data
#'   points are grouped. Must be greater than or equal to 2.
#' @param relative_error Relative error (see documentation for
#'   org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \link[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions]{here}
#'   for description). Must be in the range [0, 1]. default: 0.001
#'
#' @seealso \code{\link{ft_bucketizer}}
#' @export
ft_quantile_discretizer <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {
  UseMethod("ft_quantile_discretizer")
}

#' @export
ft_quantile_discretizer.spark_connection <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  ml_validate_args()
  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.QuantileDiscretizer",
                             input_col, output_col, uid) %>%
    invoke("setHandleInvalid", handle_invalid) %>%
    invoke("setNumBuckets", num_buckets) %>%
    invoke("setRelativeError", relative_error) %>%
    new_ml_estimator()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_quantile_discretizer.ml_pipeline <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)
}

#' @export
ft_quantile_discretizer.tbl_spark <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}
