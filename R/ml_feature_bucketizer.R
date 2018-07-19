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
#'   ft_bucketizer(input_col  = "Sepal_Length",
#'                 output_col = "Sepal_Length_bucket",
#'                 splits     = c(0, 4.5, 5, 8)) %>%
#'   select(Sepal_Length, Sepal_Length_bucket, Species)
#' }
#'
#' @export
ft_bucketizer <- function(
  x, input_col = NULL, output_col = NULL, splits = NULL,
  input_cols = NULL, output_cols = NULL, splits_array = NULL,
  handle_invalid = "error", uid = random_string("bucketizer_"), ...) {
  UseMethod("ft_bucketizer")
}

#' @export
ft_bucketizer.spark_connection <- function(
  x, input_col = NULL, output_col = NULL, splits = NULL,
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
    ml_validator_bucketizer()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.Bucketizer", .args[["uid"]])
  if (is.null(.args[["splits_array"]])) {
    jobj <- jobj %>%
      invoke("setInputCol", .args[["input_col"]]) %>%
      invoke("setOutputCol", .args[["output_col"]]) %>%
      invoke("setSplits", .args[["splits"]])
  } else {
    jobj <- jobj %>%
      invoke("setInputCols", .args[["input_cols"]]) %>%
      invoke("setOutputCols", .args[["output_cols"]])
    jobj <- invoke_static(x, "sparklyr.BucketizerUtils", "setSplitsArrayParam",
                          jobj, .args[["splits_array"]])
  }
  jobj <- jobj %>%
    jobj_set_param("setHandleInvalid", .args[["handle_invalid"]], "error", "2.1.0")

  new_ml_bucketizer(jobj)
}

#' @export
ft_bucketizer.ml_pipeline <- function(
  x, input_col = NULL, output_col = NULL, splits = NULL,
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
ft_bucketizer.tbl_spark <- function(
  x, input_col = NULL, output_col = NULL, splits = NULL,
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
  new_ml_transformer(jobj, subclass = "ml_bucketizer")
}

# Validator
ml_validator_bucketizer <- function(.args) {
  .args[["uid"]] <- forge::cast_scalar_character(.args[["uid"]])

  if (!is.null(.args[["input_col"]])) {
    if (!is.null(.args[["input_cols"]]))
      stop("Only one of `input_col` or `input_cols` may be specified.", call. = FALSE)
    .args[["input_col"]] <- forge::cast_scalar_character(.args[["input_col"]])
    .args[["output_col"]] <- forge::cast_scalar_character(.args[["output_col"]])
    if (length(.args[["splits"]]) < 3)
      stop("`splits` must be at least length 3.", call. = FALSE)
    .args[["splits"]] <- forge::cast_double(.args[["splits"]]) %>% as.list()
  } else if (!is.null(.args[["input_cols"]])) {
    .args[["input_cols"]] <- forge::cast_character(.args[["input_cols"]]) %>% as.list()
    .args[["output_cols"]] <- forge::cast_character(.args[["output_cols"]]) %>% as.list()
    .args[["splits_array"]] <- purrr::map(
      .args[["splits_array"]],
      ~ forge::cast_double(.x) %>% as.list()
    )
  } else {
    stop("One of `input_col` or `input_cols` must be specified.", call. = FALSE)
  }

  .args[["handle_invalid"]] <- forge::cast_choice(
    .args[["handle_invalid"]], c("error", "skip", "keep")
  )
  .args
}
