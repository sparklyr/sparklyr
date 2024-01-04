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
    x, input_col, output_col, threshold = 0,
    uid = random_string("binarizer_"), ...
    ) {
  check_dots_used()
  UseMethod("ft_binarizer")
}

ml_binarizer <- ft_binarizer

ft_binarizer_impl <- function(
    x, input_col = NULL, output_col = NULL, threshold = 0,
    uid = random_string("binarizer_"), ...
    ) {
  ft_process_step(
    x = x,
    uid = uid,
    r_class = "ml_binarizer",
    step_class = "ml_binarizer",
    invoke_steps = list(
      threshold = cast_scalar_double(threshold),
      input_col = cast_nullable_string(input_col),
      output_col = cast_nullable_string(output_col)
    )
  )
}

#' @export
ft_binarizer.spark_connection <- ft_binarizer_impl

#' @export
ft_binarizer.ml_pipeline <- ft_binarizer_impl

#' @export
ft_binarizer.tbl_spark <- ft_binarizer_impl
