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

ft_bucketizer_impl <- function(x, input_col = NULL, output_col = NULL,
                               splits = NULL, input_cols = NULL,
                               output_cols = NULL, splits_array = NULL,
                               handle_invalid = "error", uid = random_string("bucketizer_"),
                               ...) {

  handle_invalid <- param_min_version(x, handle_invalid, "2.1.0")
  input_cols <- param_min_version(x, input_cols, "2.3.0")
  output_cols <- param_min_version(x, output_cols, "2.3.0")

  if (!is.null(input_col) && !is.null(input_cols)) {
    stop("Only one of `input_col` or `input_cols` may be specified.", call. = FALSE)
  }

  if (!is.null(splits) && length(splits) < 3) {
    stop("`splits` must be at least length 3.", call. = FALSE)
  }

  jobj <- jobj_process_args(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.Bucketizer",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setSplits = cast_nullable_double_list(splits),
      setInputCols = cast_nullable_string_list(input_cols),
      setOutputCols = cast_nullable_string_list(output_cols),
      setHandleInvalid = cast_choice(handle_invalid, c("error", "skip", "keep"))
    )
  )

  if (!is.null(splits_array)) {
    splits_array <- purrr::map(splits_array, ~ cast_double_list(.x))
    jobj <- invoke_static(
      spark_connection(x),
      "sparklyr.BucketizerUtils",
      "setSplitsArrayParam",
      jobj,
      splits_array
    )}

  stage <- new_ml_transformer(jobj, class = "ml_bucketizer")

  ft_post_obj(x = x, stage = stage)
}

ml_bucketizer <- ft_bucketizer

#' @export
ft_bucketizer.spark_connection <- ft_bucketizer_impl

#' @export
ft_bucketizer.ml_pipeline <- ft_bucketizer_impl

#' @export
ft_bucketizer.tbl_spark <- ft_bucketizer_impl
