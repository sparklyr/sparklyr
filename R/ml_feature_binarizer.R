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
#'   ft_binarizer(input_col  = "Sepal_Length",
#'                output_col = "Sepal_Length_bin",
#'                threshold  = 5) %>%
#'   select(Sepal_Length, Sepal_Length_bin, Species)
#' }
#'
#' @export
ft_binarizer <- function(x, input_col, output_col, threshold = 0, uid = random_string("binarizer_"), ...) {
  check_dots_used()
  UseMethod("ft_binarizer")
}

ml_binarizer <- ft_binarizer

#' @export
ft_binarizer.spark_connection <- function(x, input_col = NULL, output_col = NULL, threshold = 0,
                                          uid = random_string("binarizer_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    threshold = threshold,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_binarizer()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.Binarizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setThreshold", .args[["threshold"]])

  new_ml_binarizer(jobj)
}

#' @export
ft_binarizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, threshold = 0,
                                     uid = random_string("binarizer_"), ...) {
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
ft_binarizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL, threshold = 0,
                                   uid = random_string("binarizer_"), ...) {
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
