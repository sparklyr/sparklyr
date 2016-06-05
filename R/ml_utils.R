spark_jobj_list_to_array_df <- function(data, dataNames) {
  listOfLists <- lapply(data, function(e) {
    spark_invoke(e, "toArray")
  })

  df <- as.data.frame(matrix(unlist(listOfLists), nrow=length(listOfLists)))
  colnames(df) <- dataNames

  df
}
