#' Feature Transformation -- Discrete Cosine Transform (DCT) (Transformer)
#'
#' A feature transformer that takes the 1D discrete cosine transform of a real
#'   vector. No zero padding is performed on the input vector. It returns a real
#'   vector of the same length representing the DCT. The return vector is scaled
#'   such that the transform matrix is unitary (aka scaled DCT-II).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param inverse Indicates whether to perform the inverse DCT (TRUE) or forward DCT (FALSE).
#' @export
ft_dct <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  inverse = FALSE,
  uid = random_string("dct_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_dct")
}

ml_dct <- ft_dct

#' @export
ft_dct.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  inverse = FALSE,
  uid = random_string("dct_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_dct()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.DCT",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setInverse", .args[["inverse"]])

  new_ml_dct(jobj)
}

#' @export
ft_dct.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  inverse = FALSE,
  uid = random_string("dct_"),
  ...
) {
  transformer <- ft_dct.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  )
  ml_add_stage(x, transformer)
}

#' @export
ft_dct.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  inverse = FALSE,
  uid = random_string("dct_"),
  ...
) {
  transformer <- ft_dct.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  )
  ml_transform(transformer, x)
}

new_ml_dct <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_dct")
}

#' @rdname ft_dct
#' @details \code{ft_discrete_cosine_transform()} is an alias for \code{ft_dct} for backwards compatibility.
#' @export
ft_discrete_cosine_transform <- function(
  x,
  input_col,
  output_col,
  inverse = FALSE,
  uid = random_string("dct_"),
  ...
) {
  UseMethod("ft_dct")
}

validator_ml_dct <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["inverse"]] <- cast_scalar_logical(.args[["inverse"]])
  .args
}

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
  x,
  input_col = NULL,
  output_col = NULL,
  scaling_vec = NULL,
  uid = random_string("elementwise_product_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_elementwise_product")
}

ml_elementwise_product <- ft_elementwise_product

#' @export
ft_elementwise_product.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  scaling_vec = NULL,
  uid = random_string("elementwise_product_"),
  ...
) {
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
    x,
    "org.apache.spark.ml.feature.ElementwiseProduct",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  )
  if (!is.null(.args[["scaling_vec"]])) {
    jobj <- invoke_static(
      x,
      "sparklyr.MLUtils2",
      "setScalingVec",
      jobj,
      .args[["scaling_vec"]]
    )
  }

  new_ml_elementwise_product(jobj)
}

#' @export
ft_elementwise_product.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  scaling_vec = NULL,
  uid = random_string("elementwise_product_"),
  ...
) {
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
ft_elementwise_product.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  scaling_vec = NULL,
  uid = random_string("elementwise_product_"),
  ...
) {
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
  .args[["scaling_vec"]] <- cast_double_list(
    .args[["scaling_vec"]],
    allow_null = TRUE
  )
  .args
}

#' Feature Transformation -- Interaction (Transformer)
#'
#' Implements the feature interaction transform. This transformer takes in Double and
#'   Vector type columns and outputs a flattened vector of their feature interactions.
#'   To handle interaction, we first one-hot encode any nominal features. Then, a
#'   vector of the feature cross-products is produced.
#'
#' @param input_cols The names of the input columns
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_interaction <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("interaction_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_interaction")
}

ml_interaction <- ft_interaction

#' @export
ft_interaction.spark_connection <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("interaction_"),
  ...
) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_interaction()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.Interaction",
    input_cols = .args[["input_cols"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  )

  new_ml_interaction(jobj)
}

#' @export
ft_interaction.ml_pipeline <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("interaction_"),
  ...
) {
  stage <- ft_interaction.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_interaction.tbl_spark <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("interaction_"),
  ...
) {
  stage <- ft_interaction.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_interaction <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_interaction")
}

validator_ml_interaction <- function(.args) {
  .args[["input_cols"]] <- cast_string_list(
    .args[["input_cols"]],
    allow_null = TRUE
  )
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args
}
