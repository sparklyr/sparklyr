spark_jobj_list_to_array_df <- function(data, dataNames) {
  listOfLists <- lapply(data, function(e) {
    spark_invoke(e, "toArray")
  })

  df <- as.data.frame(t(matrix(unlist(listOfLists), nrow=length(dataNames))))
  colnames(df) <- dataNames

  df
}

# Given multiple columns in a Spark DataFrame, generate
# a new column with name 'output_col' where each element
# is the concatenation of the 'input_cols'.
spark_assemble_vector <- function(sc, df, input_cols, output_col) {

  assembler <- spark_invoke_static_ctor(
    sc,
    "org.apache.spark.ml.feature.VectorAssembler"
  )

  assembler %>%
    spark_invoke("setInputCols", input_cols) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("transform", df)
}
