ml_create_mapping_tables <- function() {
  param_mapping_list <-
    list("input_col" = "inputCol",
         "output_col" = "outputCol",
         # ml_logistic_regression
         "elastic_net_param" = "elasticNetParam",
         "features_col" = "featuresCol",
         "fit_intercept" = "fitIntercept",
         "label_col" = "labelCol",
         "max_iter" = "maxIter",
         "prediction_col" = "predictionCol",
         "probability_col" = "probabilityCol",
         "raw_prediction_col" = "rawPredictionCol",
         "reg_param" = "regParam",
         "weight_col" = "weightCol",
         "aggregation_depth" = "aggregationDepth",
         "num_features" = "numFeatures",
         # ft_r_formula
         "force_index_label" = "forceIndexLabel",
         # ft_string_indexer
         "handle_invalid" = "handleInvalid",
         # ft_one_hot_encoder
         "drop_last" = "dropLast",
         # ft_vector_assembler
         "input_cols" = "inputCols",
         # ft_stop_words_remover
         "case_sensitive" = "caseSensitive",
         "stop_words" = "stopWords",
         # ft_elementwise_product
         "scaling_vec" = "scalingVec",
         # ft_regex_tokenizer
         "min_token_length" = "minTokenLength",
         "to_lower_case" = "toLowercase",
         # ft_count_vectorizer
         "min_df" = "minDF",
         "min_tf" = "minTF",
         "vocab_size" = "vocabSize",
         # ft_quantile_discretizer
         "num_buckets" = "numBuckets",
         "relative_error" = "relativeError")

  param_mapping_r_to_s <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))
  param_mapping_s_to_r <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))

  invisible(lapply(names(param_mapping_list),
                   function(x) {
                     param_mapping_r_to_s[[x]] <- param_mapping_list[[x]]
                     param_mapping_s_to_r[[param_mapping_list[[x]]]] <- x
                   }))

  ml_class_mapping_list <- list(
    "org.apache.spark.ml.feature.HashingTF" = "hashing_tf",
    "org.apache.spark.ml.classification.LogisticRegression" = "logistic_regression"
  )

  ml_class_mapping <- new.env(parent = emptyenv(),
                               size = length(ml_class_mapping_list))

  invisible(lapply(names(ml_class_mapping_list),
                   function(x) {
                     ml_class_mapping[[x]] <- ml_class_mapping_list[[x]]
                   }))

  devtools::use_data(param_mapping_r_to_s, param_mapping_s_to_r,
                     ml_class_mapping,
                     internal = TRUE, overwrite = TRUE)
}
