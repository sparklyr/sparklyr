# Given multiple columns in a Spark DataFrame, generate
# a new column with name 'output.col' where each element
# is the concatenation of the 'input.cols'.
ml_apply_vector_assembler <- function(df, input.cols, output.col)
{
  stopifnot(is.character(input.cols))

  scon <- spark_scon(df)

  assembler <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.VectorAssembler"
  )

  assembler %>%
    spark_invoke("setInputCols", as.list(input.cols)) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("transform", df)
}

# Given a string column in a Spark DataFrame, generate
# a new column with name 'output.col' where each element
# is a unique number mapping to the original column name.
# use the StringIndexer to create a categorical variable
ml_apply_string_indexer <- function(df, input.col, output.col,
                                    params = NULL)
{
  stopifnot(is.character(input.col))

  scon <- spark_scon(df)
  indexer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.StringIndexer"
  )

  sim <- indexer %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("fit", df)

  # Report labels to caller if requested -- these map
  # the discovered labels in the data set to an associated
  # index.
  if (is.environment(params))
    params$labels <- as.character(spark_invoke(sim, "labels"))

  spark_invoke(sim, "transform", df)
}

# Given a numeric column as input, generate an output column with binary
# (0/1) features.
ml_apply_binarizer <- function(df, input.col, output.col,
                               threshold = 0.5)
{
  scon <- spark_scon(df)

  binarizer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.Binarizer"
  )

  binarizer %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setThreshold", as.double(threshold)) %>%
    spark_invoke("transform", df)
}

ml_apply_discrete_cosine_transform <- function(df, input.col, output.col,
                                               inverse = FALSE)
{
  scon <- spark_scon(df)

  dct <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.DCT"
  )

  dct %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setInverse", as.logical(inverse)) %>%
    spark_invoke("transform", df)
}

ml_apply_index_to_string <- function(df, input.col, output.col)
{
  scon <- spark_scon(df)

  converter <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.IndexToString"
  )

  converter %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("transform", df)
}

ml_apply_standard_scaler <- function(df, input.col, output.col,
                                     with.mean, with.std)
{
  scon <- spark_scon(df)

  scaler <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.StandardScaler"
  )

  scaler %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setWithMean", as.logical(with.mean)) %>%
    spark_invoke("setWithStd", as.logical(with.std)) %>%
    spark_invoke("transform", df)
}

ml_apply_min_max_scaler <- function(df, input.col, output.col,
                                    min = 0, max = 1)
{
  scon <- spark_scon(df)

  scaler <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.MinMaxScaler"
  )

  scaler %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setMin", as.numeric(min)) %>%
    spark_invoke("setMax", as.numeric(max)) %>%
    spark_invoke("transform", df)
}

ml_apply_bucketizer <- function(df, input.col, output.col,
                                splits)
{
  stopifnot(is.numeric(splits))

  scon <- spark_scon(df)

  bucketizer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.Bucketizer"
  )

  bucketizer %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setSplits", as.list(splits)) %>%
    spark_invoke("transform", df)
}

ml_apply_elementwise_product <- function(df, lhs.col, rhs.col, output.col)
{
  scon <- spark_scon(df)

  transformer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.ElementwiseProduct"
  )

  transformer %>%
    spark_invoke("setInputCol", lhs.col) %>%
    spark_invoke("setScalingVec", rhs.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("transform", df)
}

ml_apply_sql_transformer <- function(df, sql)
{
  scon <- spark_scon(df)

  transformer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.SQLTransformer"
  )

  transformer %>%
    spark_invoke("setStatement", paste(sql, collapse = "\n")) %>%
    spark_invoke("transform", df)
}

ml_apply_quantile_discretizer <- function(df, input.col, output.col,
                                          n.buckets)
{
  scon <- spark_scon(df)

  discretizer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.QuantileDiscretizer"
  )

  discretizer %>%
    spark_invoke("setInputCol", input.col) %>%
    spark_invoke("setOutputCol", output.col) %>%
    spark_invoke("setNumBuckets", as.numeric(n.buckets)) %>%
    spark_invoke("fit", df) %>%
    spark_invoke("transform", df)
}
