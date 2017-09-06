ml_create_param_mapping_tables <- function() {
  param_mapping_list <-
    list("input_col" = "inputCol",
         "output_col" = "outputCol",
         "label_col" = "labelCol",
         "prediction_col" = "predictionCol",
         "probability_col" = "probabilityCol",
         "raw_prediction_col" = "rawPredictionCol",
         "features" = "featuresCol",
         "alpha" = "elasticNetParam",
         "intercept" = "fitIntercept",
         "max_iter" = "maxIter",
         "aggregation_depth" = "aggregationDepth",
         "lambda" = "regParam",
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

  devtools::use_data(param_mapping_r_to_s, param_mapping_s_to_r,
                     internal = TRUE, overwrite = TRUE)
}
