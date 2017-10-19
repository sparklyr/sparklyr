#' Feature Transformation -- Binarizer (Transformer)
#'
#' Apply thresholding to a column, such that values less than or equal to the
#' \code{threshold} are assigned the value 0.0, and values greater than the
#' threshold are assigned the value 1.0. Column output is numeric for
#' compatibility with other modeling functions.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param threshold Threshold used to binarize continuous features.
#'
#' @export
ft_binarizer <- function(x, input_col, output_col, threshold = 0, uid = random_string("binarizer_"), ...) {
  UseMethod("ft_binarizer")
}

#' @export
ft_binarizer.spark_connection <- function(x, input_col, output_col, threshold = 0,
                                          uid = random_string("binarizer_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                             input_col, output_col, uid) %>%
    invoke("setThreshold", threshold)

  new_ml_binarizer(jobj)
}

#' @export
ft_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold = 0,
                                     uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_binarizer.tbl_spark <- function(x, input_col, output_col, threshold = 0,
                                   uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_binarizer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_binarizer")
}
