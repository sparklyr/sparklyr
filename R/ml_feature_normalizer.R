#' Feature Transformation -- Normalizer (Transformer)
#'
#' Normalize a vector to have unit norm using the given p-norm.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param p Normalization in L^p space. Must be >= 1. Defaults to 2.
#'
#' @export
ft_normalizer <- function(
  x, input_col, output_col, p = 2,
  uid = random_string("normalizer_"), ...
  ) {
  UseMethod("ft_normalizer")
}

#' @export
ft_normalizer.spark_connection <- function(
  x, input_col, output_col, p = 2,
  uid = random_string("normalizer_"), ...
  ) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Normalizer",
                             input_col, output_col, uid) %>%
    invoke("setP", p)

  new_ml_normalizer(jobj)
}

#' @export
ft_normalizer.ml_pipeline <- function(
  x, input_col, output_col, p = 2,
  uid = random_string("normalizer_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_normalizer.tbl_spark <- function(
  x, input_col, output_col, p = 2,
  uid = random_string("normalizer_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_normalizer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_normalizer")
}

ml_validator_normalizer <- function(args, nms) {
  args %>%
    ml_validate_args({
      p <- ensure_scalar_double(p)
      if (p < 1) stop("'p' must be greater than or equal to 1")
    }) %>%
    ml_extract_args(nms)
}
