# RFormula

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
  transformer <- ml_new_stage_modified_args()
  ml_fit_and_transform(transformer, x)
}

# StringIndexer

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

  estimator <- ml_new_stage_modified_args()

  # backwards compatibility for params argument
  if (rlang::has_name(dots, "params") && rlang::is_env(dots$params)) {
    transformer <- ml_fit(estimator, x)
    dots$params$labels <- transformer$.jobj %>%
      invoke("labels") %>%
      as.character()
    transformer %>%
      ml_transform(x)
  } else {
    ml_fit_and_transform(estimator, x)
  }
}
