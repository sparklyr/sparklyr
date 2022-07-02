#' Feature Transformation -- Tokenizer (Transformer)
#'
#' A tokenizer that converts the input string to lowercase and then splits it
#' by white spaces.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_tokenizer <- function(x, input_col = NULL, output_col = NULL,
                         uid = random_string("tokenizer_"), ...) {
  check_dots_used()
  UseMethod("ft_tokenizer")
}

ft_tokenizer_impl <- function(x, input_col = NULL, output_col = NULL,
                              uid = random_string("tokenizer_"), ...) {
  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.Tokenizer",
    r_class = "ml_tokenizer",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col)
    )
  )
}

ml_tokenizer <- ft_tokenizer

#' @export
ft_tokenizer.spark_connection <- ft_tokenizer_impl

#' @export
ft_tokenizer.ml_pipeline <- ft_tokenizer_impl

#' @export
ft_tokenizer.tbl_spark <- ft_tokenizer_impl
