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
ft_vector_assembler <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {
  UseMethod("ft_vector_assembler")
}

#' @export
ft_vector_assembler.spark_connection <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.VectorAssembler", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCol", output_col)

  new_ml_vector_assembler(jobj)
}

#' @export
ft_vector_assembler.ml_pipeline <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_vector_assembler.tbl_spark <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_vector_assembler <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_vector_assembler")
}
