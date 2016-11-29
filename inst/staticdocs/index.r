library(staticdocs)

sd_section(
  "Connecting to Spark",
  "Functions for installing Spark components and managing connections to Spark.",
  c(
    "spark_config",
    "spark_connect",
    "spark_disconnect",
    "spark_install",
    "spark_log",
    "spark_web"
  )
)

sd_section(
  "Reading and Writing Data",
  "Functions for reading and writing Spark DataFrames.",
  c(
    "spark_read_csv",
    "spark_read_json",
    "spark_read_parquet",
    "spark_read_table",
    "spark_write_csv",
    "spark_write_json",
    "spark_write_parquet",
    "spark_write_table"
  )
)

sd_section(
  "Spark Tables",
  "Functions for manipulating Spark Tables.",
  c(
    "tbl_cache",
    "tbl_uncache"
  )
)

sd_section(
  "Spark DataFrames",
  "Functions for maniplulating Spark DataFrames.",
  c(
    "sdf_copy_to",
    "sdf_mutate",
    "sdf_partition",
    "sdf_predict",
    "sdf_read_column",
    "sdf_register",
    "sdf_sample",
    "sdf_sort",
    "sdf_with_unique_id"
  )
)

sd_section(
  "Machine Learning Algorithms",
  "Functions for invoking machine learning algorithms.",
  c(
    "ml_als_factorization",
    "ml_decision_tree",
    "ml_generalized_linear_regression",
    "ml_gradient_boosted_trees",
    "ml_kmeans",
    "ml_lda",
    "ml_linear_regression",
    "ml_logistic_regression",
    "ml_multilayer_perceptron",
    "ml_naive_bayes",
    "ml_one_vs_rest",
    "ml_pca",
    "ml_random_forest",
    "ml_survival_regression"
  )
)

sd_section(
  "Machine Learning Transformers",
  "Functions for transforming features in Spark DataFrames.",
  c(
    "ft_binarizer",
    "ft_bucketizer",
    "ft_discrete_cosine_transform",
    "ft_elementwise_product",
    "ft_index_to_string",
    "ft_one_hot_encoder",
    "ft_quantile_discretizer",
    "ft_sql_transformer",
    "ft_string_indexer",
    "ft_vector_assembler"
  )
)

sd_section(
  "Machine Learning Utilities",
  "Functions for interacting with Spark ML model fits.",
  c(
    "ml_binary_classification_eval",
    "ml_classification_eval",
    "ml_tree_feature_importance",
    "ml_saveload"
  )
)

sd_section(
  "Machine Learning Extensions",
  "Functions for creating custom wrappers to other Spark ML algorithms.",
  c(
    "ml_create_dummy_variables",
    "ml_model",
    "ml_options",
    "ml_prepare_dataframe",
    "ml_prepare_response_features_intercept"
  )
)

sd_section(
  "Extensions API",
  "Functions for creating extensions to the sparklyr package.",
  c(
    "compile_package_jars",
    "connection_config",
    "find_scalac",
    "hive_context",
    "invoke",
    "java_context",
    "register_extension",
    "spark_compilation_spec",
    "spark_default_compilation_spec",
    "spark_connection",
    "spark_context",
    "spark_dataframe",
    "spark_dependency",
    "spark_jobj",
    "spark_session",
    "spark_version"
  )
)
