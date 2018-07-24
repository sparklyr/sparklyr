ml_create_mapping_tables <- function() { # nocov start
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
         "lower_bounds_on_coefficients" = "lowerBoundsOnCoefficients",
         "upper_bounds_on_coefficients" = "upperBoundsOnCoefficients",
         "lower_bounds_on_intercepts" = "lowerBoundsOnIntercepts",
         "upper_bounds_on_intercepts" = "upperBoundsOnIntercepts",
         # ft_standard_scaler
         "with_mean" = "withMean",
         "with_std" = "withStd",
         # ft_vector_indexer
         "max_categories" = "maxCategories",
         # ft_imputer
         "missing_value" = "missingValue",
         "output_cols" = "outputCols",
         # ft_word2vec
         "vector_size" = "vectorSize",
         "min_count" = "minCount",
         "max_sentence_length" = "maxSentenceLength",
         "num_partitions" = "numPartitions",
         # ft_chisq_selector
         "num_top_features" = "numTopFeatures",
         "selector_type" = "selectorType",
         # ft_bucketed_random_projection_lsh
         "bucket_length" = "bucketLength",
         "num_hash_tables" = "numHashTables",
         # ft_idf
         "min_doc_freq" = "minDocFreq",
         # ft_r_formula
         "force_index_label" = "forceIndexLabel",
         # ft_string_indexer
         "handle_invalid" = "handleInvalid",
         "string_order_type" = "stringOrderType",
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
         "num_buckets_array" = "numBucketsArray",
         "relative_error" = "relativeError",
         # ft_bucketizer
         "splits_array" = "splitsArray",
         # ft_feature-hasher
         "categorical_cols" = "categoricalCols",
         # ml_generalized_linear_regression
         "link_prediction_col" = "linkPredictionCol",
         "variance_power" = "variancePower",
         "link_power" = "linkPower",
         "offset_col" = "offsetCol",
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
         # ml_fpgrowth
         "items_col" = "itemsCol",
         "min_confidence" = "minConfidence",
         "min_support" = "minSupport",
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
         "topic_distribution_col" = "topicDistributionCol",
         "keep_last_checkpoint" = "keepLastCheckpoint",
         "learning_decay" = "learningDecay",
         "learning_offset" = "learningOffset",
         "optimize_doc_concentration" = "optimizeDocConcentration",
         "topic_distribution_col" = "topicDistributionCol",
         # ml_kmeans
         "max_iter" = "maxIter",
         "init_steps" = "initSteps",
         "init_mode" = "initMode",
         # ml_bisecting_kmeans
         "min_divisible_cluster_size" = "minDivisibleClusterSize",
         # evaluators
         "metric_name" = "metricName",
         # tuning
         "collect_sub_models" = "collectSubModels",
         "num_folds" = "numFolds",
         "train_ratio" = "trainRatio")

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
    # feature (transformers)
    "PolynomialExpansion" = "polynomial_expansion",
    "Normalizer" = "normalizer",
    "Interaction" = "interaction",
    "HashingTF" = "hashing_tf",
    "Binarizer" = "binarizer",
    "Bucketizer" = "bucketizer",
    "DCT" = "dct",
    "ElementwiseProduct" = "elementwise_product",
    "IndexToString" = "index_to_string",
    "OneHotEncoder" = "one_hot_encoder",
    "RegexTokenizer" = "regex_tokenizer",
    "SQLTransformer" = "sql_transformer",
    "StopWordsRemover" = "stop_words_remover",
    "Tokenizer" = "tokenizer",
    "VectorAssembler" = "vector_assembler",
    "NGram" = "ngram",
    "VectorSlicer" = "vector_slicer",
    "FeatureHasher" = "feature_hasher",
    # feature (estimators)
    "VectorIndexer" = "vector_indexer",
    "VectorIndexerModel" = "VectorIndexerModel",
    "StandardScaler" = "standard_scaler",
    "StandardScalerModel" = "standard_scaler_model",
    "MinMaxScaler" = "min_max_scaler",
    "MinMaxScalerModel" = "min_max_scaler_model",
    "MaxAbsScaler" = "max_abs_scaler",
    "MaxAbsScalerModel" = "max_abs_scaler_model",
    "Imputer" = "imputer",
    "ImputerModel" = "imputer_model",
    "ChiSqSelector" = "chisq_selector",
    "ChiSqSelectorModel" = "chisq_selector_model",
    "Word2Vec" = "word2vec",
    "Word2VecModel" = "word2vec_model",
    "IDF" = "idf",
    "IDFModel" = "idf_model",
    "VectorIndexer" = "vector_indexer",
    "VectorIndexerModel" = "vector_indexer_model",
    "QuantileDiscretizer" = "quantile_discretizer",
    "RFormula" = "r_formula",
    "RFormulaModel" = "r_formula_model",
    "StringIndexer" = "string_indexer",
    "StringIndexerModel" = "string_indexer_model",
    "CountVectorizer" = "count_vectorizer",
    "CountVectorizerModel" = "count_vectorizer_model",
    "PCA" = "pca",
    "PCAModel" = "pca_model",
    "BucketedRandomProjectionLSH" = "bucketed_random_projection_lsh",
    "BucketedRandomProjectionLSHModel" = "bucketed_random_projection_lsh_model",
    "MinHashLSH" = "minhash_lsh",
    "MinHashLSHModel" = "minhash_lsh_model",
    # regression
    "LogisticRegression" = "logistic_regression",
    "LogisticRegressionModel" = "logistic_regression_model",
    "LinearRegression" = "linear_regression",
    "LinearRegressionModel" = "linear_regression_model",
    "GeneralizedLinearRegression" = "generalized_linear_regression",
    "GeneralizedLinearRegressionModel" = "generalized_linear_regression_model",
    "DecisionTreeRegressor" = "decision_tree_regressor",
    "DecisionTreeRegressionModel" = "decision_tree_regression_model",
    "GBTRegressor" = "gbt_regressor",
    "GBTRegressionModel" = "gbt_regression_model",
    "RandomForestRegressor" = "random_forest_regressor",
    "RandomForestRegressionModel" = "random_forest_regression_model",
    "AFTSurvivalRegression" = "aft_survival_regression",
    "AFTSurvivalRegressionModel" = "aft_survival_regression_model",
    "IsotonicRegression" = "isotonic_regression",
    "IsotonicRegressionModel" = "isotonic_regression_model",
    # classification
    "GBTClassifier" = "gbt_classifier",
    "GBTClassificationModel" = "gbt_classification_model",
    "DecisionTreeClassifier" = "decision_tree_classifier",
    "DecisionTreeClassificationModel" = "decision_tree_classification_model",
    "RandomForestClassifier" = "random_forest_classifier",
    "RandomForestClassificationModel" = "random_forest_classification_model",
    "NaiveBayes" = "naive_bayes",
    "NaiveBayesModel" = "naive_bayes_model",
    "MultilayerPerceptronClassifier" = "multilayer_perceptron_classifier",
    "MultilayerPerceptronClassificationModel" = "multilayer_perceptron_classification_model",
    "OneVsRest" = "one_vs_rest",
    "OneVsRestModel" = "one_vs_rest_model",
    "LinearSVC" = "linear_svc",
    "LinearSVCModel" = "linear_svc_model",
    # recommendation
    "ALS" = "als",
    "ALSModel" = "als_model",
    # clustering
    "LDA" = "lda",
    "LDAModel" = "lda_model",
    "KMeans" = "kmeans",
    "KMeansModel" = "kmeans_model",
    "BisectingKMeans" = "bisecting_kmeans",
    "BisectingKMeansModel" = "bisecting_kmeans_model",
    "GaussianMixture" = "gaussian_mixture",
    "GaussianMixtureModel" = "gaussian_mixture_model",
    # fpm
    "FPGrowth" = "fpgrowth",
    "FPGrowthModel" = "fpgrowth_model",
    # tuning
    "CrossValidator" = "cross_validator",
    "CrossValidatorModel" = "cross_validator_model",
    "TrainValidationSplit" = "train_validation_split",
    "TrainValidationSplitModel" = "train_validation_split_model",
    # evaluation
    "BinaryClassificationEvaluator" = "binary_classification_evaluator",
    "MulticlassClassificationEvaluator" = "multiclass_classification_evaluator",
    "RegressionEvaluator" = "regression_evaluator",
    "ClusteringEvaluator" = "clustering_evaluator",
    # pipeline
    "Pipeline" = "pipeline",
    "PipelineModel" = "pipeline_model",
    "Transformer" = "transformer",
    "Estimator" = "estimator",
    "PipelineStage" = "pipeline_stage"
  )

  ml_class_mapping <- new.env(parent = emptyenv(),
                               size = length(ml_class_mapping_list))

  invisible(lapply(names(ml_class_mapping_list),
                   function(x) {
                     ml_class_mapping[[x]] <- ml_class_mapping_list[[x]]
                   }))

  rlang::ll(param_mapping_r_to_s = param_mapping_r_to_s,
            param_mapping_s_to_r = param_mapping_s_to_r,
            ml_class_mapping = ml_class_mapping)
} # nocov end
