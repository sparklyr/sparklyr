#' Feature Transformation -- VectorSlicer (Transformer)
#'
#' Takes a feature vector and outputs a new feature vector with a subarray of the original features.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param indices An vector of indices to select features from a vector column.
#'   Note that the indices are 0-based.
#' @export
ft_vector_slicer <- function(
  x, input_col, output_col, indices,
  uid = random_string("vector_slicer_"), ...
) {
  UseMethod("ft_vector_slicer")
}

#' @export
ft_vector_slicer.spark_connection <- function(
  x, input_col, output_col, indices,
  uid = random_string("vector_slicer_"), ...
) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.VectorSlicer",
                             input_col, output_col, uid) %>%
    invoke("setIndices", indices)

  new_ml_vector_slicer(jobj)
}

#' @export
ft_vector_slicer.ml_pipeline <- function(
  x, input_col, output_col, indices,
  uid = random_string("vector_slicer_"), ...
) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

ml_validator_vector_slicer <- function(args, nms) {
  args %>%
    ml_validate_args({
      indices <- lapply(indices, ensure_scalar_integer)
    }) %>%
    ml_extract_args(nms)
}

#' @export
ft_vector_slicer.tbl_spark <- function(
  x, input_col, output_col, indices,
  uid = random_string("vector_slicer_"), ...
) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_vector_slicer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_vector_slicer")
}
