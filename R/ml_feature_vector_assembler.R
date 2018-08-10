#' Feature Transformation -- VectorAssembler (Transformer)
#'
#' Combine multiple vectors into a single row-vector; that is,
#' where each row element of the newly generated column is a
#' vector formed by concatenating each row element from the
#' specified input columns.
#'
#' @param input_cols The names of the input columns
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_vector_assembler <- function(x, input_cols = NULL, output_col = NULL,
                                uid = random_string("vector_assembler_"), ...) {
  UseMethod("ft_vector_assembler")
}

#' @export
ft_vector_assembler.spark_connection <- function(x, input_cols = NULL, output_col = NULL,
                                                 uid = random_string("vector_assembler_"), ...) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_vector_assembler()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.VectorAssembler",
    input_cols = .args[["input_cols"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  )

  new_ml_vector_assembler(jobj)
}

#' @export
ft_vector_assembler.ml_pipeline <- function(x, input_cols = NULL, output_col = NULL,
                                            uid = random_string("vector_assembler_"), ...) {
  stage <- ft_vector_assembler.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_vector_assembler.tbl_spark <- function(x, input_cols = NULL, output_col = NULL,
                                          uid = random_string("vector_assembler_"), ...) {
  stage <- ft_vector_assembler.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_vector_assembler <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_vector_assembler")
}

ml_validator_vector_assembler <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    input.col = "input_cols",
    output.col = "output_col"
  )) %>%
    validate_args_transformer()
  .args
}
