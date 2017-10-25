#' Feature Transformation -- IndexToString (Transformer)
#'
#' A Transformer that maps a column of indices back to a new column of
#'   corresponding string values. The index-string mapping is either from
#'   the ML attributes of the input column, or from user-supplied labels
#'    (which take precedence over ML attributes). This function is the inverse
#'    of \code{\link{ft_string_indexer}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param labels Optional param for array of labels specifying index-string mapping.
#' @seealso \code{\link{ft_string_indexer}}
#' @export
ft_index_to_string <- function(x, input_col, output_col, labels = NULL,
                               uid = random_string("index_to_string_"), ...) {
  UseMethod("ft_index_to_string")
}

#' @export
ft_index_to_string.spark_connection <- function(
  x, input_col, output_col, labels = NULL, uid = random_string("index_to_string_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.IndexToString",
                             input_col, output_col, uid)

  if (!rlang::is_null(labels))
    jobj <- invoke(jobj, "setLabels", labels)

  new_ml_index_to_string(jobj)
}

#' @export
ft_index_to_string.ml_pipeline <- function(
  x, input_col, output_col, labels = NULL,
  uid = random_string("index_to_string_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_index_to_string.tbl_spark <- function(
  x, input_col, output_col, labels = NULL,
  uid = random_string("index_to_string_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_index_to_string <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_index_to_string")
}
