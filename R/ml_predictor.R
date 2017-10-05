ml_new_predictor <- function(sc, class, uid, features_col, label_col,
                             prediction_col) {
  ensure_scalar_character(features_col)
  ensure_scalar_character(label_col)
  ensure_scalar_character(prediction_col)

  invoke_new(sc, class, uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col)
}

ml_new_classifier <- function(sc, class, uid,
                              features_col,
                              label_col,
                              prediction_col,
                              probability_col,
                              raw_prediction_col, ...) {
  ensure_scalar_character(probability_col)
  ensure_scalar_character(raw_prediction_col)
  ml_new_predictor(sc, class, id, features_col, label_col,
                   prediction_col) %>%
    invoke("setProbabilityCol", probability_col) %>%
    invoke("setRawPredictionCol", raw_prediction_col)
}

ml_new_regressor <- function(sc, class, uid,
                              features_col, label_col, prediction_col,
                              ...) {
  ml_new_predictor(sc, class, uid, features_col, label_col,
                   prediction_col)
}
