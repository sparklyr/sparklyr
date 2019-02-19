#' Feature Transformation -- StringIndexer (Estimator)
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
#' @param string_order_type (Spark 2.3+)How to order labels of string column.
#'  The first label after ordering is assigned an index of 0. Options are
#'  \code{"frequencyDesc"}, \code{"frequencyAsc"}, \code{"alphabetDesc"}, and \code{"alphabetAsc"}.
#'  Defaults to \code{"frequencyDesc"}.
#' @seealso \code{\link{ft_index_to_string}}
#' @export
ft_string_indexer <- function(x, input_col = NULL, output_col = NULL,
                              handle_invalid = "error", string_order_type = "frequencyDesc",
                              uid = random_string("string_indexer_"), ...) {
  check_dots_used()
  UseMethod("ft_string_indexer")
}

ml_string_indexer <- ft_string_indexer

#' @export
ft_string_indexer.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                               handle_invalid = "error", string_order_type = "frequencyDesc",
                                               uid = random_string("string_indexer_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid
  ) %>%
    validator_ml_string_indexer()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.StringIndexer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setHandleInvalid", .args[["handle_invalid"]], "2.1.0", "error") %>%
    jobj_set_param("setStringOrderType", .args[["string_order_type"]], "2.3.0",  "frequencyDesc") %>%
    new_ml_string_indexer()

  estimator

}

#' @export
ft_string_indexer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                          handle_invalid = "error", string_order_type = "frequencyDesc",
                                          uid = random_string("string_indexer_"), ...) {

  stage <- ft_string_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_string_indexer.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                        handle_invalid = "error", string_order_type = "frequencyDesc",
                                        uid = random_string("string_indexer_"), ...) {
  stage <- ft_string_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid,
    ...
  )
  # backwards compatibility for params argument
  dots <- rlang::dots_list(...)
  if (rlang::has_name(dots, "params") && rlang::is_env(dots$params)) {
    warning("`params` has been deprecated and will be removed in a future release.", call. = FALSE)
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
  new_ml_estimator(jobj, class = "ml_string_indexer")
}

new_ml_string_indexer_model <- function(jobj) {
  new_ml_transformer(jobj,
                     labels = invoke(jobj, "labels") %>%
                       as.character(),
                     class = "ml_string_indexer_model")
}

#' @rdname ft_string_indexer
#' @param model A fitted StringIndexer model returned by \code{ft_string_indexer()}
#' @return \code{ml_labels()} returns a vector of labels, corresponding to indices to be assigned.
#' @export
ml_labels <- function(model) model$labels

validator_ml_string_indexer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]], c("error", "skip", "keep")
  )
  .args[["string_order_type"]] <- cast_choice(
    .args[["string_order_type"]],
    c("frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc")
  )
  .args
}
