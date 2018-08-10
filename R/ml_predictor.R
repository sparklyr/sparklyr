ml_new_predictor <- function(sc, class, uid, features_col, label_col,
                             prediction_col) {
  uid <- cast_string(uid)
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
  uid <- cast_string(uid)
  jobj <- invoke_new(sc, class, uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setK", k) %>%
    invoke("setMaxIter", max_iter) %>%
    maybe_set_param("setSeed", seed)
}
