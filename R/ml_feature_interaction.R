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

ft_interaction_impl <- function(x, input_cols = NULL, output_col = NULL,
                                uid = random_string("interaction_"), ...) {

  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.Interaction",
    r_class = "ml_interaction",
    invoke_steps = list(
      setInputCols = cast_nullable_string_list(input_cols),
      setOutputCol = cast_nullable_string(output_col)
    )
  )
}

ml_interaction <- ft_interaction

#' @export
ft_interaction.spark_connection <- ft_interaction_impl

#' @export
ft_interaction.ml_pipeline <- ft_interaction_impl

#' @export
ft_interaction.tbl_spark <- ft_interaction_impl
