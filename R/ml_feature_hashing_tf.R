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

ml_hashing_tf <- ft_hashing_tf

#' @export
ft_hashing_tf.spark_connection <- function(x, input_col = NULL, output_col = NULL, binary = FALSE,
                                           num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_hashing_tf()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.HashingTF",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setBinary", .args[["binary"]], "2.0.0", FALSE) %>%
    invoke("setNumFeatures", .args[["num_features"]])

  new_ml_hashing_tf(jobj)
}

#' @export
ft_hashing_tf.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, binary = FALSE,
                                      num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  stage <- ft_hashing_tf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_hashing_tf.tbl_spark <- function(x, input_col = NULL, output_col = NULL, binary = FALSE,
                                    num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  stage <- ft_hashing_tf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_hashing_tf <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_hashing_tf")
}

validator_ml_hashing_tf <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["binary"]] <- cast_scalar_logical(.args[["binary"]])
  .args[["num_features"]] <- cast_scalar_integer(.args[["num_features"]])
  .args
}
