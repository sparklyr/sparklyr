#' Feature Transformation -- ElementwiseProduct (Transformer)
#'
#' Outputs the Hadamard product (i.e., the element-wise product) of each input vector
#'   with a provided "weight" vector. In other words, it scales each column of the
#'   dataset by a scalar multiplier.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param scaling_vec the vector to multiply with input vectors
#'
#' @export
ft_elementwise_product <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {
  if (spark_version(spark_connection(x)) < "2.0.0")
    stop("'ft_elementwise_product()' is only supported for Spark 2.0+")
  UseMethod("ft_elementwise_product")
}

#' @export
ft_elementwise_product.spark_connection <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.ElementwiseProduct",
                             input_col, output_col, uid) %>%
    (function(jobj) invoke_static(x,
                                  "sparklyr.MLUtils2",
                                  "setScalingVec",
                                  jobj, scaling_vec))

  new_ml_elementwise_product(jobj)
}

#' @export
ft_elementwise_product.ml_pipeline <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_elementwise_product.tbl_spark <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_elementwise_product <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_elementwise_product")
}
