spark_jobj_list_to_array_df <- function(data, dataNames) {
  listOfLists <- lapply(data, function(e) {
    spark_invoke(e, "toArray")
  })

  df <- as.data.frame(t(matrix(unlist(listOfLists), nrow=length(dataNames))))
  colnames(df) <- dataNames

  df
}

ml_prepare_dataframe <- function(df, features, response = NULL, ...,
                                 envir = new.env(parent = emptyenv()))
{
  df <- as_spark_dataframe(df)
  schema <- spark_dataframe_schema(df)

  # default report for feature, response variable names
  envir$features <- random_string("features")
  envir$response <- response
  envir$labels <- NULL

  # ensure numeric response
  if (!is.null(response)) {
    responseType <- schema[[response]]$type
    if (responseType == "StringType") {
      envir$response <- random_string("response")
      df <- ft_string_indexer(df, response, envir$response, envir)
    } else if (responseType != "DoubleType") {
      envir$response <- random_string("response")
      df <- spark_dataframe_cast_column(df, response, envir$response, "DoubleType")
    }
  }

  # assemble features vector and return
  ft_vector_assembler(df, features, envir$features)
}

try_null <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' @export
predict.ml_model <- function(object, newdata, ...) {
  sdf <- as_spark_dataframe(newdata)
  params <- object$model.parameters
  assembled <- ft_vector_assembler(sdf, object$features, params$features)
  predicted <- spark_invoke(object$.model, "transform", assembled)
  column <- spark_dataframe_read_column(predicted, "prediction")
  if (is.character(params$labels) && is.numeric(column))
    column <- params$labels[column + 1]
  column
}

#' @export
fitted.ml_model <- function(object, ...) {
  object$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("predictions") %>%
    spark_dataframe_read_column("prediction")
}

#' @export
residuals.ml_model <- function(object, ...) {
  object$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("residuals") %>%
    spark_dataframe_read_column("residuals")
}
