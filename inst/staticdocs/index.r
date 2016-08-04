sd_section("Connecting to Spark",
           "Functions for installing Spark components and managing connections to Spark.",
           c("spark_install",
             "spark_connect",
             "spark_log",
             "spark_web",
             "spark_disconnect",
             "spark_config")
)

sd_section("Reading and Writing Data",
           "Functions for reading and writing Spark DataFrames",
           c("spark_read_csv",
             "spark_read_json",
             "spark_read_parquet",
             "spark_write_csv",
             "spark_write_json",
             "spark_write_parquet")
)

sd_section("dplyr Interface",
           "Functions implementing a dplyr backend for Spark DataFrames",
           c("copy_to",
             "tbl_cache",
             "tbl_uncache")
)

sd_section("Spark DataFrames",
           "Functions for maniplulating Spark DataFrames",
           c("sdf_copy_to",
             "sdf_partition",
             "sdf_mutate",
             "sdf_sample",
             "sdf_sort",
             "sdf_predict",
             "sdf_register",
             "sdf_with_unique_id")
)

sd_section("Machine Learning Algorithms.",
           "Functions for invoking machine learning algorithms.",
           c("ml_kmeans",
             "ml_linear_regression",
             "ml_logistic_regression",
             "ml_survival_regression",
             "ml_generalized_linear_regression",
             "ml_decision_tree",
             "ml_random_forest",
             "ml_gradient_boosted_trees",
             "ml_pca",
             "ml_naive_bayes",
             "ml_multilayer_perceptron",
             "ml_lda",
             "ml_one_vs_rest",
             "ml_als_factorization",
             "ml_saveload")
)

sd_section("Machine Learning Transformers",
           "Functions for transforming features in Spark DataFrames",
           c("ft_binarizer",
             "ft_bucketizer",
             "ft_discrete_cosine_transform",
             "ft_elementwise_product",
             "ft_index_to_string",
             "ft_quantile_discretizer",
             "ft_sql_transformer",
             "ft_string_indexer",
             "ft_vector_assembler",
             "ft_one_hot_encoder"
           )
)

sd_section("Machine Learning Utility Functions",
           "Functions for creating custom wrappers to other Spark ML algorithms",
           c("ensure",
             "ml_model",
             "ml_prepare_dataframe",
             "ml_prepare_response_features_intercept",
             "ml_create_dummy_variables"
           )
)

sd_section("Extensions API",
           "Functions for creating extensions to the sparklyr package",
           c("invoke",
             "spark_connection",
             "connection_config",
             "spark_version",
             "spark_jobj",
             "spark_dataframe",
             "spark_context",
             "java_context",
             "hive_context",
             "register_extension",
             "spark_dependency")
)




