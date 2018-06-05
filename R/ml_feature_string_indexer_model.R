#' @rdname ft_string_indexer
#' @param labels Vector of labels, corresponding to indices to be assigned.
#' @export
ft_string_indexer_model <- function(
  x, input_col, output_col, labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"), ...) {
  UseMethod("ft_string_indexer_model")
}

#' @export
ft_string_indexer_model.spark_connection <- function(
  x, input_col, output_col, labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.StringIndexerModel",
                     uid, labels) %>%
    invoke("setInputCol", input_col) %>%
    invoke("setOutputCol", output_col)

  new_ml_string_indexer_model(jobj)
}

#' @export
ft_string_indexer_model.ml_pipeline <- function(
  x, input_col, output_col, labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"), ...
) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_string_indexer_model.tbl_spark <- function(
  x, input_col, output_col, labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"), ...
) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

ml_validator_string_indexer_model <- function(args, nms) {
  args %>%
    ml_validate_args({
      labels <- lapply(labels, ensure_scalar_character)
      handle_invalid <- rlang::arg_match(
        handle_invalid, c("error", "skip", "keep"))
    }) %>%
    ml_extract_args(nms)
}
