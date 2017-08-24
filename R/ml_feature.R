#' @export
ml_tokenizer <- function(sc, input_col, output_col, name) {
  jobj <- invoke_new(sc,
                     "org.apache.spark.ml.feature.Tokenizer") %>%
    invoke("setInputCol", input_col) %>%
    invoke("setOutputCol", output_col)
  pipeline_stage <- setNames(list(
    list(
      .stage = jobj)
  ), name)

  out <- list(
    stages = pipeline_stage)

  out
}
