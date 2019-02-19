#' Feature Transformation -- VectorSlicer (Transformer)
#'
#' Takes a feature vector and outputs a new feature vector with a subarray of the original features.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param indices An vector of indices to select features from a vector column.
#'   Note that the indices are 0-based.
#' @export
ft_vector_slicer <- function(x, input_col = NULL, output_col = NULL, indices = NULL,
                             uid = random_string("vector_slicer_"), ...) {
  check_dots_used()
  UseMethod("ft_vector_slicer")
}

ml_vector_slicer <- ft_vector_slicer

#' @export
ft_vector_slicer.spark_connection <- function(x, input_col = NULL, output_col = NULL, indices = NULL,
                                              uid = random_string("vector_slicer_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_vector_slicer()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.VectorSlicer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setIndices", .args[["indices"]])

  new_ml_vector_slicer(jobj)
}

#' @export
ft_vector_slicer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, indices = NULL,
                                         uid = random_string("vector_slicer_"), ...) {
  stage <- ft_vector_slicer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_vector_slicer.tbl_spark <- function(x, input_col = NULL, output_col = NULL, indices = NULL,
                                       uid = random_string("vector_slicer_"), ...) {
  stage <- ft_vector_slicer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_vector_slicer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_vector_slicer")
}

validator_ml_vector_slicer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["indices"]] <- cast_nullable_integer_list(.args[["indices"]])
  .args
}
