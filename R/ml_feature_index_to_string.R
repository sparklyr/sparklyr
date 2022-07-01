#' Feature Transformation -- IndexToString (Transformer)
#'
#' A Transformer that maps a column of indices back to a new column of
#'   corresponding string values. The index-string mapping is either from
#'   the ML attributes of the input column, or from user-supplied labels
#'    (which take precedence over ML attributes). This function is the inverse
#'    of \code{\link{ft_string_indexer}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param labels Optional param for array of labels specifying index-string mapping.
#' @seealso \code{\link{ft_string_indexer}}
#' @export
ft_index_to_string <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                               uid = random_string("index_to_string_"), ...) {
  check_dots_used()
  UseMethod("ft_index_to_string")
}

ft_index_to_string_impl <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                                    uid = random_string("index_to_string_"), ...) {
  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.IndexToString",
    r_class = "ml_index_to_string",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setLabels = cast_nullable_string_list(labels)
    )
  )
}

ml_index_to_string <- ft_index_to_string

#' @export
ft_index_to_string.spark_connection <- ft_index_to_string_impl

#' @export
ft_index_to_string.ml_pipeline <- ft_index_to_string_impl

#' @export
ft_index_to_string.tbl_spark <- ft_index_to_string_impl
