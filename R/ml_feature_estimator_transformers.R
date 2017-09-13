# RFormula

#' @export
ml_r_formula <- function(x, formula, features_col = "features", label_col = "label",
                         force_index_label = FALSE,
                         uid = random_string("r_formula_"), ...) {
  UseMethod("ml_r_formula")
}

#' @export
ml_r_formula.spark_connection <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE,
  uid = random_string("r_formula_"), ...) {

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.RFormula", uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setForceIndexLabel", force_index_label) %>%
    invoke("setFormula", formula) %>%
    invoke("setLabelCol", label_col)

  if (is.null(dataset))
    ml_info(jobj)
  else {
    jobj %>%
      ml_fit(dataset) %>%
      ml_transformer_info()
  }
}

#' @export
ml_r_formula.ml_pipeline <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE,
  uid = random_string("r_formula_"), ...
) {

  stage <- ml_new_stage_modified_args(rlang::call_frame())
  # FIXME following line should be redundant, remove after tests pass
  # stage <- if (is.null(dataset)) transformer else ml_fit(transformer, dataset)
  ml_add_stage(x, stage)

}

#' @export
ml_r_formula.tbl_spark <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE,
  uid = random_string("r_formula_"), ...
) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_fit_and_transform(transformer, x)
}

# StringIndexer

#' @export
ml_string_indexer <- function(
  x, input_col, output_col,
  handle_invalid = c("error", "skip", "keep"),
  uid = random_string("string_indexer_"), ...) {
  UseMethod("ml_string_indexer")
}

#' @export
ml_string_indexer.spark_connection <- function(
  x, input_col, output_col,
  handle_invalid = c("error", "skip", "keep"),
  uid = random_string("string_indexer_"), ...) {

  ml_validate_args(rlang::caller_env())

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.StringIndexer",
                             input_col, output_col, uid) %>%
    invoke("setHandleInvalid", handle_invalid)

  if (is.null(dataset))
    ml_info(jobj)
  else {
    jobj %>%
      ml_fit(dataset) %>%
      ml_transformer_info()
  }
}

#' @export
ml_string_indexer.ml_pipeline <- function(
  x, input_col, output_col,
  handle_invalid = c("error", "skip", "keep"),
  uid = random_string("string_indexer_"), ...
) {

  stage <- ml_new_stage_modified_args(rlang::call_frame())
  # FIXME following line should be redundant, remove after tests pass
  # stage <- if (is.null(dataset)) transformer else ml_fit(transformer, dataset)
  ml_add_stage(x, stage)

}

#' @export
ml_string_indexer.tbl_spark <- function(
  x, input_col, output_col,
  handle_invalid = c("error", "skip", "keep"),
  uid = random_string("string_indexer_"), ...
) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_fit_and_transform(transformer, x)
}
