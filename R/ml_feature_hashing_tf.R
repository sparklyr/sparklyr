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
ft_hashing_tf <- function(x, input_col = NULL, output_col = NULL, binary = FALSE,
                          num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  check_dots_used()
  UseMethod("ft_hashing_tf")
}

ft_hashing_tf_impl <- function(x, input_col = NULL, output_col = NULL,
                               binary = FALSE, num_features = 2^18,
                               uid = random_string("hashing_tf_"), ...) {

  binary <- param_min_version(x, binary, "2.0.0")

  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.HashingTF",
    r_class = "ml_hashing_tf",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setBinary = cast_scalar_logical(binary),
      setNumFeatures = cast_scalar_integer(num_features)
    )
  )
}

ml_hashing_tf <- ft_hashing_tf

#' @export
ft_hashing_tf.spark_connection <- ft_hashing_tf_impl

#' @export
ft_hashing_tf.ml_pipeline <- ft_hashing_tf_impl

#' @export
ft_hashing_tf.tbl_spark <- ft_hashing_tf_impl
