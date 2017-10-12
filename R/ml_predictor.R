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
  ml_new_predictor(sc, class, uid, features_col, label_col,
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

ml_new_clustering <- function(sc, class, uid,
                              features_col, k, max_iter, seed = NULL, ...) {
  ensure_scalar_character(features_col)
  k <- ensure_scalar_integer(k)
  max_iter <- ensure_scalar_integer(max_iter)
  seed <- ensure_scalar_integer(seed, allow.null = TRUE)

  jobj <- invoke_new(sc, class, uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setK", k) %>%
    invoke("setMaxIter", max_iter)

  if (rlang::is_null(seed))
    jobj
  else
    invoke(jobj, "setSeed", seed)
}
