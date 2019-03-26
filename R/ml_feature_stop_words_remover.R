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
#'   swedish, turkish. Defaults to English. See \url{http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/}
#'   for more details
#'
#' @return A list of stop words.
#'
#' @seealso \code{\link{ft_stop_words_remover}}
#' @export
ml_default_stop_words <- function(sc, language = c("english", "danish", "dutch", "finnish",
                                                   "french", "german", "hungarian", "italian",
                                                   "norwegian", "portuguese", "russian", "spanish",
                                                   "swedish", "turkish"), ...) {
  language <- rlang::arg_match(language)
  invoke_static(sc, "org.apache.spark.ml.feature.StopWordsRemover",
                "loadDefaultStopWords", language)
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

ml_stop_words_remover <- ft_stop_words_remover

#' @export
ft_stop_words_remover.spark_connection <- function(x, input_col = NULL, output_col = NULL, case_sensitive = FALSE,
                                                   stop_words = ml_default_stop_words(spark_connection(x), "english"),
                                                   uid = random_string("stop_words_remover_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_stop_words_remover()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.StopWordsRemover",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setCaseSensitive", .args[["case_sensitive"]]) %>%
    invoke("setStopWords", .args[["stop_words"]])

  new_ml_stop_words_remover(jobj)
}

#' @export
ft_stop_words_remover.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, case_sensitive = FALSE,
                                              stop_words = ml_default_stop_words(spark_connection(x), "english"),
                                              uid = random_string("stop_words_remover_"), ...) {
  stage <- ft_stop_words_remover.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_stop_words_remover.tbl_spark <- function(x, input_col = NULL, output_col = NULL, case_sensitive = FALSE,
                                            stop_words = ml_default_stop_words(spark_connection(x), "english"),
                                            uid = random_string("stop_words_remover_"), ...) {
  stage <- ft_stop_words_remover.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_stop_words_remover <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_stop_words_remover")
}

validator_ml_stop_words_remover <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["case_sensitive"]] <- cast_scalar_logical(.args[["case_sensitive"]])
  .args[["stop_words"]] <- cast_character_list(.args[["stop_words"]])
  .args
}
