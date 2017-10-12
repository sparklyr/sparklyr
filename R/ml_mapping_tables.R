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
         "relative_error" = "relativeError",
         # ml_generalized_linear_regression
         "link_prediction_col" = "linkPredictionCol",
         "variance_power" = "variancePower",
         "link_power" = "linkPower",
         # ml_decision_tree_regressor
         "variance_col" = "varianceCol",
         "checkpoint_interval" = "checkpointInterval",
         "max_bins" = "maxBins",
         "max_depth" = "maxDepth",
         "min_info_gain" = "minInfoGain",
         "min_instances_per_node" = "minInstancesPerNode",
         "cache_node_ids" = "cacheNodeIds",
         "max_memory_in_mb" = "maxMemoryInMB",
         # ml_gbt_classifier
         "loss_type" = "lossType",
         "step_size" = "stepSize",
         "subsampling_rate" = "subsamplingRate",
         # ml_random_forest_classifier
         "num_trees" = "numTrees",
         "feature_subset_strategy" = "featureSubsetStrategy",
         # ml_naive_bayes
         "model_type" = "modelType",
         # ml_multilayer_perceptron_classifier
         "block_size" = "blockSize",
         "initial_weights" = "initialWeights",
         # ml_aft_survival_regression
         "censor_col" = "censorCol",
         "quantile_probabilities" = "quantileProbabilities",
         "quantiles_col" = "quantilesCol",
         # ml_isotonic_regression
         "feature_index" = "featureIndex",
         # ml_als
         "rating_col" = "ratingCol",
         "user_col" = "userCol",
         "item_col" = "itemCol",
         "implicit_prefs" = "implicitPrefs",
         "num_user_blocks" = "numUserBlocks",
         "num_item_blocks" = "numItemBlocks",
         "cold_start_strategy" = "coldStartStrategy",
         "intermediate_storage_level" = "intermediateStorageLevel",
         "final_storage_level" = "finalStorageLevel",
         # ml_lda
         "doc_concentration" = "docConcentration",
         "topic_concentration" = "topicConcentration",
         "keep_last_checkpoint" = "keepLastCheckpoint",
         "learning_decay" = "learningDecay",
         "learning_offset" = "learningOffset",
         "optimize_doc_concentration" = "optimizeDocConcentration",
         "topic_distribution_col" = "topicDistributionCol",
         # ml_kmeans
         "max_iter" = "maxIter",
         "init_steps" = "initSteps",
         "init_mode" = "initMode")

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
