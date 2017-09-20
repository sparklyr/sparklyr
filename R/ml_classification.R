ml_new_classifier <- function(sc, class,
                              uid = uid,
                              label_col,
                              prediction_col,
                              probability_col,
                              raw_prediction_col, ...) {
  ensure_scalar_character(label_col)
  ensure_scalar_character(prediction_col)
  ensure_scalar_character(probability_col)
  ensure_scalar_character(raw_prediction_col)
  invoke_new(sc, class, uid) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setProbabilityCol", probability_col) %>%
    invoke("setRawPredictionCol", raw_prediction_col)
}
