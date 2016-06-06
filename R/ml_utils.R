spark_jobj_list_to_array_df <- function(data, dataNames) {
  listOfLists <- lapply(data, function(e) {
    spark_invoke(e, "toArray")
  })

  df <- as.data.frame(t(matrix(unlist(listOfLists), nrow=length(dataNames))))
  colnames(df) <- dataNames

  df
}
