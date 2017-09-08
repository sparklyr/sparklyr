ml_create_mapping_tables <- function() {
  param_mapping_list <-
    list("input_col" = "inputCol",
         "output_col" = "outputCol",
         "label_col" = "labelCol",
         "prediction_col" = "predictionCol",
         "probability_col" = "probabilityCol",
         "raw_prediction_col" = "rawPredictionCol",
         "features_col" = "featuresCol",
         "elastic_net_param" = "elasticNetParam",
         "fit_intercept" = "fitIntercept",
         "max_iter" = "maxIter",
         "aggregation_depth" = "aggregationDepth",
         "reg_param" = "regParam",
         "num_features" = "numFeatures")

  param_mapping_r_to_s <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))
  param_mapping_s_to_r <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))

  invisible(lapply(names(param_mapping_list),
                   function(x) {
                     param_mapping_r_to_s[[x]] <- param_mapping_list[[x]]
                     param_mapping_s_to_r[[param_mapping_list[[x]]]] <- x
                   }))

  validator_mapping_list <- list(
    "org.apache.spark.ml.feature.HashingTF" = "ml_validator_hashing_tf",
    "org.apache.spark.ml.classification.LogisticRegression" = "ml_validator_logistic_regression"
  )

  validator_mapping <- new.env(parent = emptyenv(),
                               size = length(validator_mapping_list))

  invisible(lapply(names(validator_mapping_list),
                   function(x) {
                     validator_mapping[[x]] <- validator_mapping_list[[x]]
                   }))

  devtools::use_data(param_mapping_r_to_s, param_mapping_s_to_r,
                     validator_mapping,
                     internal = TRUE, overwrite = TRUE)
}
