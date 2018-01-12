#' Feature Transformation -- Interaction (Transformer)
#'
#' Implements the feature interaction transform. This transformer takes in Double and
#'   Vector type columns and outputs a flattened vector of their feature interactions.
#'   To handle interaction, we first one-hot encode any nominal features. Then, a
#'   vector of the feature cross-products is produced.
#'
#' @param input_cols The names of the input columns
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_interaction <- function(
  x, input_cols, output_col,
  uid = random_string("interaction_"), ...) {
  UseMethod("ft_interaction")
}

#' @export
ft_interaction.spark_connection <- function(
  x, input_cols, output_col,
  uid = random_string("interaction_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.Interaction", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCol", output_col)

  new_ml_interaction(jobj)
}

#' @export
ft_interaction.ml_pipeline <- function(
  x, input_cols, output_col,
  uid = random_string("interaction_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_interaction.tbl_spark <- function(
  x, input_cols, output_col,
  uid = random_string("interaction_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_interaction <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_interaction")
}

ml_validator_interaction <- function(args, nms) {
  old_new_mapping <- list(
    input.col = "input_cols",
    output.col = "output_col"
  )

  args %>%
    ml_validate_args({
      input_cols <- input_cols %>%
        lapply(ensure_scalar_character)
      output_col <- ensure_scalar_character(output_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}
