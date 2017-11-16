#' Feature Transformation -- HashingTF (Transformer)
#'
#' Maps a sequence of terms to their term frequencies using the hashing trick.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param binary Binary toggle to control term frequency counts.
#'   If true, all non-zero counts are set to 1. This is useful for discrete
#'   probabilistic models that model binary events rather than integer
#'   counts. (default = \code{FALSE})
#' @param num_features Number of features. Should be greater than 0. (default = \code{2^18})
#'
#' @export
ft_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = as.integer(2^18), uid = random_string("hashing_tf_"), ...) {
  UseMethod("ft_hashing_tf")
}

#' @export
ft_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = as.integer(2^18), uid = random_string("hashing_tf_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.HashingTF",
                             input_col, output_col, uid) %>%
    jobj_set_param("setBinary", binary, FALSE, "2.0.0") %>%
    invoke("setNumFeatures", num_features)

  new_ml_hashing_tf(jobj)
}

#' @export
ft_hashing_tf.ml_pipeline <- function(x, input_col, output_col, binary = FALSE,
                                      num_features = as.integer(2^18), uid = random_string("hashing_tf_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_hashing_tf.tbl_spark <- function(x, input_col, output_col, binary = FALSE,
                                    num_features = as.integer(2^18), uid = random_string("hashing_tf_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_hashing_tf <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_hashing_tf")
}
