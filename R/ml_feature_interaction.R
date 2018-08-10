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
ft_interaction <- function(x, input_cols = NULL, output_col = NULL,
                           uid = random_string("interaction_"), ...) {
  UseMethod("ft_interaction")
}

#' @export
ft_interaction.spark_connection <- function(x, input_cols = NULL, output_col = NULL,
                                            uid = random_string("interaction_"), ...) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_interaction()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.Interaction",
    input_cols = .args[["input_cols"]], output_col = .args[["output_col"]], uid = .args[["uid"]])

  new_ml_interaction(jobj)
}

#' @export
ft_interaction.ml_pipeline <- function(x, input_cols = NULL, output_col = NULL,
                                       uid = random_string("interaction_"), ...) {

  stage <- ft_interaction.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_interaction.tbl_spark <- function(x, input_cols = NULL, output_col = NULL,
                                     uid = random_string("interaction_"), ...) {

  stage <- ft_interaction.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_interaction <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_interaction")
}

ml_validator_interaction <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    input.col = "input_cols",
    output.col = "output_col"
  ))
  .args[["input_cols"]] <- cast_nullable_string_list(.args[["input_cols"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args
}
