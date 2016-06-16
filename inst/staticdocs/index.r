sd_section("Connecting to Spark",
           "Functions for installing Spark components and managing connections to Spark.",
           c("spark_install",
             "spark_connect",
             "spark_disconnect",
             "spark_config",
             "spark_log",
             "spark_web")
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
           ac("src_spark",
             "copy_to",
             "collect",
             "tbl_cache",
             "tbl_uncache")
)

sd_section("MLlib Interface",
           "Functions for invoking MLlib algorithms.",
           c("ml_kmeans",
             "ml_linear_regression",
             "ml_logistic_regression",
             "ml_random_forest",
             "ml_pca",
             "ml_multilayer_perceptron",
             "df_partition",
             "df_mutate",
             "ft_binarizer",
             "ft_bucketizer",
             "ft_discrete_cosine_transform",
             "ft_elementwise_product",
             "ft_index_to_string",
             "ft_quantile_discretizer",
             "ft_sql_transformer",
             "ft_string_indexer",
             "ft_vector_assembler"
           )
)

sd_section("Extensions",
           "Functions for interacting directly with the Spark Scala API",
           c("spark_context",
             "spark_invoke",
             "spark_invoke_static",
             "spark_invoke_static_ctor",
             "as_spark_dataframe")
)

sd_section("EC2",
           "Functions for creating and managing Spark EC2 clusters",
           c("spark_ec2_cluster",
             "spark_ec2_deploy",
             "spark_ec2_start",
             "spark_ec2_stop",
             "spark_ec2_master",
             "spark_ec2_login",
             "spark_ec2_web",
             "spark_ec2_rstudio",
             "spark_ec2_destroy")
)

