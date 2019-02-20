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
ft_elementwise_product <- function(x, input_col = NULL, output_col = NULL, scaling_vec = NULL,
  uid = random_string("elementwise_product_"), ...) {
  check_dots_used()
  UseMethod("ft_elementwise_product")
}

ml_elementwise_product <- ft_elementwise_product

#' @export
ft_elementwise_product.spark_connection <- function(x, input_col = NULL, output_col = NULL, scaling_vec = NULL,
                                                    uid = random_string("elementwise_product_"), ...) {
  spark_require_version(x, "2.0.0", "ElementwiseProduct")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    scaling_vec = scaling_vec,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_elementwise_product()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.ElementwiseProduct",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]])
  if (!is.null(.args[["scaling_vec"]]))
    jobj <- invoke_static(x, "sparklyr.MLUtils2", "setScalingVec", jobj, .args[["scaling_vec"]])

  new_ml_elementwise_product(jobj)
}

#' @export
ft_elementwise_product.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, scaling_vec = NULL,
                                               uid = random_string("elementwise_product_"), ...) {
  transformer <- ft_elementwise_product.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    scaling_vec = scaling_vec,
    uid = uid,
    ...
  )
  ml_add_stage(x, transformer)
}

#' @export
ft_elementwise_product.tbl_spark <- function(x, input_col = NULL, output_col = NULL, scaling_vec = NULL,
                                             uid = random_string("elementwise_product_"), ...) {
  transformer <- ft_elementwise_product.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    scaling_vec = scaling_vec,
    uid = uid,
    ...
  )
  ml_transform(transformer, x)
}

new_ml_elementwise_product <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_elementwise_product")
}

# ElementwiseProduct
validator_ml_elementwise_product <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["scaling_vec"]] <- cast_nullable_double_list(.args[["scaling_vec"]])
  .args
}
