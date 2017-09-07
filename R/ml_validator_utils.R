ml_get_stage_validator <- function(cl) {
  if (identical(cl, "org.apache.spark.ml.feature.HashingTF"))
    "ml_validator_hashing_tf"
  else if (identical(cl, "org.apache.spark.ml.classification.LogisticRegression"))
    "ml_validator_logistic_regression"
}
