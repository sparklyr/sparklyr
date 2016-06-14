# Given multiple columns in a Spark DataFrame, generate
# a new column with name 'output_col' where each element
# is the concatenation of the 'input_cols'.
ml_apply_vector_assembler <- function(df, input_cols, output_col) {
  stopifnot(is.character(input_cols))
  sc <- spark_scon(df)

  assembler <- spark_invoke_static_ctor(
    sc,
    "org.apache.spark.ml.feature.VectorAssembler"
  )

  assembler %>%
    spark_invoke("setInputCols", as.list(input_cols)) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("transform", df)
}

# Given a string column in a Spark DataFrame, generate
# a new column with name 'output_col' where each element
# is a unique number mapping to the original column name.
# use the StringIndexer to create a categorical variable
ml_apply_string_indexer <- function(df, input_col, output_col, params = NULL) {
  stopifnot(is.character(input_col))

  scon <- spark_scon(df)
  indexer <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.StringIndexer"
  )

  sim <- indexer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("fit", df)

  # Report labels to caller if requested -- these map
  # the discovered labels in the data set to an associated
  # index.
  if (is.environment(params))
    params$labels <- as.character(spark_invoke(sim, "labels"))

  spark_invoke(sim, "transform", df)
}
