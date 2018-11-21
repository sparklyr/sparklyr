ml_new_predictor <- function(sc, class, uid, features_col, label_col,
                             prediction_col) {
  uid <- cast_string(uid)
  invoke_new(sc, class, uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col)
}

spark_pipeline_stage <- function(sc, class, uid, features_col = NULL, label_col = NULL, prediction_col = NULL,
                                 probability_col = NULL, raw_prediction_col = NULL,
                                 k = NULL, max_iter = NULL, seed = NULL, input_col = NULL, input_cols = NULL,
                                 output_col = NULL, output_cols = NULL) {
  uid <- cast_string(uid)
  invoke_new(sc, class, uid) %>%
    jobj_set_ml_params(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      k = k,
      max_iter = max_iter,
      seed = seed,
      input_col = input_col,
      input_cols = input_cols,
      output_col = output_col,
      output_cols = output_cols
    )
}

jobj_set_ml_params <- function(jobj, features_col, label_col, prediction_col,
                               probability_col, raw_prediction_col,
                               k, max_iter, seed, input_col, input_cols,
                               output_col, output_cols) {
  jobj %>%
    maybe_set_param("setFeaturesCol", features_col) %>%
    maybe_set_param("setLabelCol", label_col) %>%
    maybe_set_param("setPredictionCol", prediction_col) %>%
    maybe_set_param("setProbabilityCol", probability_col) %>%
    maybe_set_param("setRawPredictionCol", raw_prediction_col) %>%
    maybe_set_param("setK", k) %>%
    maybe_set_param("setMaxIter", max_iter) %>%
    maybe_set_param("setSeed", seed) %>%
    maybe_set_param("setInputCol", input_col) %>%
    maybe_set_param("setInputCols", input_cols) %>%
    maybe_set_param("setOutputCol", output_col) %>%
    maybe_set_param("setOutputCols", output_cols)
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
