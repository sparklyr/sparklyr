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
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_normalizer")
}

ml_normalizer <- ft_normalizer

#' @export
ft_normalizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_normalizer()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.Normalizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setP", .args[["p"]])

  new_ml_normalizer(jobj)
}

#' @export
ft_normalizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  stage <- ft_normalizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_normalizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  stage <- ft_normalizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_normalizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_normalizer")
}

validator_ml_normalizer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["p"]] <- cast_scalar_double(.args[["p"]])
  if (.args[["p"]] < 1) {
    stop("`p` must be at least 1.")
  }
  .args
}

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
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' iris_tbl %>%
#'   ft_binarizer(
#'     input_col = "Sepal_Length",
#'     output_col = "Sepal_Length_bin",
#'     threshold = 5
#'   ) %>%
#'   select(Sepal_Length, Sepal_Length_bin, Species)
#' }
#'
#' @export
ft_binarizer <- function(
  x,
  input_col,
  output_col,
  threshold = 0,
  uid = random_string("binarizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_binarizer")
}

ml_binarizer <- ft_binarizer

#' @export
ft_binarizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  threshold = 0,
  uid = random_string("binarizer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    threshold = threshold,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_binarizer()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.Binarizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setThreshold", .args[["threshold"]])

  new_ml_binarizer(jobj)
}

#' @export
ft_binarizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  threshold = 0,
  uid = random_string("binarizer_"),
  ...
) {
  stage <- ft_binarizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    threshold = threshold,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_binarizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  threshold = 0,
  uid = random_string("binarizer_"),
  ...
) {
  stage <- ft_binarizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    threshold = threshold,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_binarizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_binarizer")
}

validator_ml_binarizer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["threshold"]] <- cast_scalar_double(.args[["threshold"]])
  .args
}

#' Feature Transformation -- Bucketizer (Transformer)
#'
#' Similar to \R's \code{\link{cut}} function, this transforms a numeric column
#' into a discretized column, with breaks specified through the \code{splits}
#' parameter.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-handle-invalid
#'
#' @param input_cols Names of input columns.
#' @param output_cols Names of output columns.
#' @param splits A numeric vector of cutpoints, indicating the bucket boundaries.
#' @param splits_array Parameter for specifying multiple splits parameters. Each
#'    element in this array can be used to map continuous features into buckets.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' iris_tbl %>%
#'   ft_bucketizer(
#'     input_col = "Sepal_Length",
#'     output_col = "Sepal_Length_bucket",
#'     splits = c(0, 4.5, 5, 8)
#'   ) %>%
#'   select(Sepal_Length, Sepal_Length_bucket, Species)
#' }
#'
#' @export
ft_bucketizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  splits = NULL,
  input_cols = NULL,
  output_cols = NULL,
  splits_array = NULL,
  handle_invalid = "error",
  uid = random_string("bucketizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_bucketizer")
}

ml_bucketizer <- ft_bucketizer

#' @export
ft_bucketizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  splits = NULL,
  input_cols = NULL,
  output_cols = NULL,
  splits_array = NULL,
  handle_invalid = "error",
  uid = random_string("bucketizer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    splits = splits,
    input_cols = input_cols,
    output_cols = output_cols,
    splits_array = splits_array,
    handle_invalid = handle_invalid,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_bucketizer()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.Bucketizer",
    .args[["uid"]],
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]]
  ) %>%
    jobj_set_param("setSplits", .args[["splits"]]) %>%
    jobj_set_param("setInputCols", .args[["input_cols"]], "2.3.0") %>%
    jobj_set_param("setOutputCols", .args[["output_cols"]], "2.3.0") %>%
    jobj_set_param(
      "setHandleInvalid",
      .args[["handle_invalid"]],
      "2.1.0",
      "error"
    )
  if (!is.null(.args[["splits_array"]])) {
    jobj <- invoke_static(
      x,
      "sparklyr.BucketizerUtils",
      "setSplitsArrayParam",
      jobj,
      .args[["splits_array"]]
    )
  }

  new_ml_bucketizer(jobj)
}

#' @export
ft_bucketizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  splits = NULL,
  input_cols = NULL,
  output_cols = NULL,
  splits_array = NULL,
  handle_invalid = "error",
  uid = random_string("bucketizer_"),
  ...
) {
  stage <- ft_bucketizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    splits = splits,
    input_cols = input_cols,
    output_cols = output_cols,
    splits_array = splits_array,
    handle_invalid = handle_invalid,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_bucketizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  splits = NULL,
  input_cols = NULL,
  output_cols = NULL,
  splits_array = NULL,
  handle_invalid = "error",
  uid = random_string("bucketizer_"),
  ...
) {
  stage <- ft_bucketizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    splits = splits,
    input_cols = input_cols,
    output_cols = output_cols,
    splits_array = splits_array,
    handle_invalid = handle_invalid,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_bucketizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_bucketizer")
}

# Validator
validator_ml_bucketizer <- function(.args) {
  .args[["uid"]] <- cast_string(.args[["uid"]])

  if (!is.null(.args[["input_col"]]) && !is.null(.args[["input_cols"]])) {
    stop(
      "Only one of `input_col` or `input_cols` may be specified.",
      call. = FALSE
    )
  }
  .args[["input_col"]] <- cast_nullable_string(.args[["input_col"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  if (!is.null(.args[["splits"]]) && length(.args[["splits"]]) < 3) {
    stop("`splits` must be at least length 3.", call. = FALSE)
  }
  .args[["splits"]] <- cast_double_list(.args[["splits"]], allow_null = TRUE)
  .args[["input_cols"]] <- cast_string_list(
    .args[["input_cols"]],
    allow_null = TRUE
  )
  .args[["output_cols"]] <- cast_string_list(
    .args[["output_cols"]],
    allow_null = TRUE
  )
  if (!is.null(.args[["splits_array"]])) {
    .args[["splits_array"]] <- purrr::map(
      .args[["splits_array"]],
      ~ cast_double_list(.x)
    )
  }
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args
}

#' Feature Transformation -- PolynomialExpansion (Transformer)
#'
#' Perform feature expansion in a polynomial space. E.g. take a 2-variable feature
#'   vector as an example: (x, y), if we want to expand it with degree 2, then
#'   we get (x, x * x, y, x * y, y * y).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param degree The polynomial degree to expand, which should be greater
#'   than equal to 1. A value of 1 means no expansion. Default: 2
#' @export
ft_polynomial_expansion <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_polynomial_expansion")
}

ml_polynomial_expansion <- ft_polynomial_expansion

#' @export
ft_polynomial_expansion.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_polynomial_expansion()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.PolynomialExpansion",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setDegree", .args[["degree"]])

  new_ml_polynomial_expansion(jobj)
}

#' @export
ft_polynomial_expansion.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  stage <- ft_polynomial_expansion.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_polynomial_expansion.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  stage <- ft_polynomial_expansion.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_polynomial_expansion <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_polynomial_expansion")
}

validator_ml_polynomial_expansion <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["degree"]] <- cast_scalar_integer(.args[["degree"]])
  if (.args[["degree"]] < 1) {
    stop("`degree` must be greater than 1.", call. = FALSE)
  }
  .args
}
