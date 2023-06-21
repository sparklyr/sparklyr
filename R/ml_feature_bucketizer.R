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
ft_bucketizer <- function(x, input_col = NULL, output_col = NULL, splits = NULL,
                          input_cols = NULL, output_cols = NULL, splits_array = NULL,
                          handle_invalid = "error", uid = random_string("bucketizer_"), ...) {
  check_dots_used()
  UseMethod("ft_bucketizer")
}

ml_bucketizer <- ft_bucketizer

#' @export
ft_bucketizer.spark_connection <- function(x, input_col = NULL, output_col = NULL, splits = NULL,
                                           input_cols = NULL, output_cols = NULL, splits_array = NULL,
                                           handle_invalid = "error", uid = random_string("bucketizer_"), ...) {
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
    x, "org.apache.spark.ml.feature.Bucketizer", .args[["uid"]],
    input_col = .args[["input_col"]], output_col = .args[["output_col"]]
  ) %>%
    jobj_set_param("setSplits", .args[["splits"]]) %>%
    jobj_set_param("setInputCols", .args[["input_cols"]], "2.3.0") %>%
    jobj_set_param("setOutputCols", .args[["output_cols"]], "2.3.0") %>%
    jobj_set_param("setHandleInvalid", .args[["handle_invalid"]], "2.1.0", "error")
  if (!is.null(.args[["splits_array"]])) {
    jobj <- invoke_static(
      x, "sparklyr.BucketizerUtils", "setSplitsArrayParam",
      jobj, .args[["splits_array"]]
    )
  }

  new_ml_bucketizer(jobj)
}

#' @export
ft_bucketizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, splits = NULL,
                                      input_cols = NULL, output_cols = NULL, splits_array = NULL,
                                      handle_invalid = "error", uid = random_string("bucketizer_"), ...) {
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
ft_bucketizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL, splits = NULL,
                                    input_cols = NULL, output_cols = NULL, splits_array = NULL,
                                    handle_invalid = "error", uid = random_string("bucketizer_"), ...) {
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
    stop("Only one of `input_col` or `input_cols` may be specified.", call. = FALSE)
  }
  .args[["input_col"]] <- cast_nullable_string(.args[["input_col"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  if (!is.null(.args[["splits"]]) && length(.args[["splits"]]) < 3) {
    stop("`splits` must be at least length 3.", call. = FALSE)
  }
  .args[["splits"]] <- cast_double_list(.args[["splits"]], allow_null = TRUE)
  .args[["input_cols"]] <- cast_string_list(.args[["input_cols"]], allow_null = TRUE)
  .args[["output_cols"]] <- cast_string_list(.args[["output_cols"]], allow_null = TRUE)
  if (!is.null(.args[["splits_array"]])) {
    .args[["splits_array"]] <- purrr::map(
      .args[["splits_array"]],
      ~ cast_double_list(.x)
    )
  }
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]], c("error", "skip", "keep")
  )
  .args
}
