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
  check_dots_used()
  UseMethod("ft_interaction")
}

ml_interaction <- ft_interaction

#' @export
ft_interaction.spark_connection <- function(x, input_cols = NULL, output_col = NULL,
                                            uid = random_string("interaction_"), ...) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_interaction()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.Interaction",
    input_cols = .args[["input_cols"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  )

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
  new_ml_transformer(jobj, class = "ml_interaction")
}

validator_ml_interaction <- function(.args) {
  .args[["input_cols"]] <- cast_string_list(.args[["input_cols"]], allow_null = TRUE)
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args
}
