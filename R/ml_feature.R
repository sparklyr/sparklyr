#' @export
ml_tokenizer <- function(sc, input_col, output_col, name) {
  .stage <- invoke_new(sc,
                     "org.apache.spark.ml.feature.Tokenizer") %>%
    invoke("setInputCol", input_col) %>%
    invoke("setOutputCol", output_col)
  pipeline_stage <- setNames(list(
    list(
      name = name,
      type = .stage %>%
        invoke("getClass") %>%
        invoke("getName"),
      .stage = .stage)
  ), name)

  .pipeline <- ml_wrap_in_pipeline(.stage)

  out <- list(
    stages = pipeline_stage,
    .pipeline = .pipeline)
  class(out) <- c("ml_pipeline", class(out))
  out
}
