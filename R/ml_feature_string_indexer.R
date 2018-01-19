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

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.StringIndexer",
                                  input_col, output_col, uid) %>%
    jobj_set_param("setHandleInvalid", handle_invalid, "error", "2.1.0") %>%
    new_ml_string_indexer()

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
    dots$params$labels <- spark_jobj(transformer) %>%
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

new_ml_string_indexer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_string_indexer")
}

new_ml_string_indexer_model <- function(jobj) {
  new_ml_transformer(jobj,
                     labels = invoke(jobj, "labels") %>%
                       as.character(),
                     subclass = "ml_string_indexer_model")
}

#' @rdname ft_string_indexer
#' @param model A fitted StringIndexer model returned by \code{ft_string_indexer()}
#' @return \code{ml_labels()} returns a vector of labels, corresponding to indices to be assigned.
#' @export
ml_labels <- function(model) model$labels
