#' @export
ml_tokenizer <- function(sc, input_col, output_col, name = NULL) {
  .stage <- invoke_new(sc,
                     "org.apache.spark.ml.feature.Tokenizer") %>%
    invoke("setInputCol", input_col) %>%
    invoke("setOutputCol", output_col)

  ml_pipeline(.stage, name)
}
