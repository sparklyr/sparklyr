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
#'   swedish, turkish. See \url{http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/}
#'   for more details
#'
#' @return A list of stop words.
#'
#' @seealso \code{\link{ft_stop_words_remover}}
#' @export
ml_default_stop_words <- function(
  sc, language = c("danish", "dutch", "english", "finnish",
                   "french", "german", "hungarian", "italian",
                   "norwegian", "portuguese", "russian", "spanish",
                   "swedish", "turkish"), ...) {
  language <- rlang::arg_match(language)
  invoke_static(sc, "org.apache.spark.ml.feature.StopWordsRemover",
                "loadDefaultStopWords", language)
}

#' Feature Tranformation -- StopWordsRemover (Transformer)
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
ft_stop_words_remover <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {
  UseMethod("ft_stop_words_remover")
}

#' @export
ft_stop_words_remover.spark_connection <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.StopWordsRemover",
                             input_col, output_col, uid) %>%
    invoke("setCaseSensitive", case_sensitive) %>%
    invoke("setStopWords", stop_words)

  new_ml_stop_words_remover(jobj)
}

#' @export
ft_stop_words_remover.ml_pipeline <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_stop_words_remover.tbl_spark <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_stop_words_remover <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_stop_words_remover")
}
