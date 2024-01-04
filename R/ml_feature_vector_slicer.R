#' Feature Transformation -- VectorSlicer (Transformer)
#'
#' Takes a feature vector and outputs a new feature vector with a subarray of
#' the original features.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param indices An vector of indices to select features from a vector column.
#'   Note that the indices are 0-based.
#' @export
ft_vector_slicer <- function(
    x,
    input_col = NULL,
    output_col = NULL,
    indices = NULL,
    uid = random_string("vector_slicer_"),
    ...) {
  check_dots_used()
  UseMethod("ft_vector_slicer")
}

ml_vector_slicer <- ft_vector_slicer

ft_vector_slicer_impl <- function(
    x,
    input_col = NULL,
    output_col = NULL,
    indices = NULL,
    uid = random_string("vector_slicer_"),
    ...) {
  ft_process_step(
    x = x,
    uid = uid,
    r_class = "ml_vector_slicer",
    step_class = "ml_vector_slicer",
    invoke_steps = list(
      indices = cast_integer_list(indices, allow_null = TRUE),
      input_col = cast_nullable_string(input_col),
      output_col = cast_nullable_string(output_col)
    )
  )
}

#' @export
ft_vector_slicer.spark_connection <- ft_vector_slicer_impl

#' @export
ft_vector_slicer.ml_pipeline <- ft_vector_slicer_impl

#' @export
ft_vector_slicer.tbl_spark <- ft_vector_slicer_impl
