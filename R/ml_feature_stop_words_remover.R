#' Default stop words
#'
#' Loads the default stop words for the given language.
#'
#' @param sc A \code{spark_connection}
#' @param language A character string.
#' @template roxlate-ml-dots
#'
#' @details Supported languages: danish, dutch, english, finnish, french,
#'   german, hungarian, italian, norwegian, portuguese, russian, spanish,
#'   swedish, turkish. Defaults to English. See \url{https://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/}
#'   for more details
#'
#' @return A list of stop words.
#'
#' @seealso \code{\link{ft_stop_words_remover}}
#' @export
ml_default_stop_words <- function(sc, language = c(
                                    "english", "danish", "dutch", "finnish",
                                    "french", "german", "hungarian", "italian",
                                    "norwegian", "portuguese", "russian", "spanish",
                                    "swedish", "turkish"
                                  ), ...) {
  language <- rlang::arg_match(language)
  invoke_static(
    sc, "org.apache.spark.ml.feature.StopWordsRemover",
    "loadDefaultStopWords", language
  )
}

#' Feature Transformation -- StopWordsRemover (Transformer)
#'
#' A feature transformer that filters out stop words from input.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param case_sensitive Whether to do a case sensitive comparison over the stop words.
#' @param stop_words The words to be filtered out.
#'
#' @seealso \code{\link{ml_default_stop_words}}
#'
#' @export
ft_stop_words_remover <- function(x, input_col = NULL, output_col = NULL, case_sensitive = FALSE,
                                  stop_words = ml_default_stop_words(spark_connection(x), "english"),
                                  uid = random_string("stop_words_remover_"), ...) {
  check_dots_used()
  UseMethod("ft_stop_words_remover")
}

ft_stop_words_remover_impl <- function(x, input_col = NULL, output_col = NULL, case_sensitive = FALSE,
                                       stop_words = ml_default_stop_words(spark_connection(x), "english"),
                                       uid = random_string("stop_words_remover_"), ...) {
  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.StopWordsRemover",
    r_class = "ml_stop_words_remover",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setCaseSensitive = cast_scalar_logical(case_sensitive),
      setStopWords = cast_character_list(stop_words)
    )
  )
}

ml_stop_words_remover <- ft_stop_words_remover

#' @export
ft_stop_words_remover.spark_connection <- ft_stop_words_remover_impl
#' @export
ft_stop_words_remover.ml_pipeline <- ft_stop_words_remover_impl

#' @export
ft_stop_words_remover.tbl_spark <- ft_stop_words_remover_impl

