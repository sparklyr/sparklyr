spark_jobj_list_to_array_df <- function(data, dataNames) {
  listOfLists <- lapply(data, function(e) {
    sparkapi_invoke(e, "toArray")
  })

  df <- as.data.frame(t(matrix(unlist(listOfLists), nrow=length(dataNames))))
  colnames(df) <- dataNames

  df
}

ml_prepare_dataframe <- function(df, features, response = NULL, ...,
                                 envir = new.env(parent = emptyenv()))
{
  df <- sparkapi_dataframe(df)
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
      castedColumn <- df %>%
        sparkapi_invoke("col", response) %>%
        sparkapi_invoke("cast", "double")
      df <- df %>%
        sparkapi_invoke("withColumn", envir$response, castedColumn)
    }
  }

  # assemble features vector and return
  transformed <- ft_vector_assembler(df, features, envir$features)
  
  # return as vanilla spark dataframe
  sparkapi_dataframe(transformed)
}

try_null <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' @export
predict.ml_model <- function(object, newdata, ...) {
  params <- object$model.parameters
  predicted <- sdf_predict(object, newdata, ...)
  column <- spark_dataframe_read_column(predicted, "prediction")
  if (is.character(params$labels) && is.numeric(column))
    column <- params$labels[column + 1]
  column
}

#' @export
fitted.ml_model <- function(object, ...) {
  object$.model %>%
    sparkapi_invoke("summary") %>%
    sparkapi_invoke("predictions") %>%
    spark_dataframe_read_column("prediction")
}

#' @export
residuals.ml_model <- function(object, ...) {
  object$.model %>%
    sparkapi_invoke("summary") %>%
    sparkapi_invoke("residuals") %>%
    spark_dataframe_read_column("residuals")
}

reorder_first <- function(vector, name) {
  if (is.null(vector))
    return(vector)
  
  nm <- names(vector)
  if (is.null(nm) || !name %in% nm)
    return(vector)
  
  ordered <- c(name, base::setdiff(nm, name)) 
  vector[ordered]
}

intercept_first <- function(vector) {
  reorder_first(vector, "(Intercept)")
}

read_spark_vector <- function(jobj, field) {
  object <- sparkapi_invoke(jobj, field)
  sparkapi_invoke(object, "toArray")
}

read_spark_matrix <- function(jobj, field) {
  object <- sparkapi_invoke(jobj, field)
  nrow <- sparkapi_invoke(object, "numRows")
  ncol <- sparkapi_invoke(object, "numCols")
  data <- sparkapi_invoke(object, "toArray")
  matrix(data, nrow = nrow, ncol = ncol)
}

ensure_scalar_integer <- function(object) {
  
  if (length(object) != 1 || !is.numeric(object)) {
    deparsed <- deparse(substitute(object))
    errMsg <- sprintf("'%s' is not a length-one numeric value", deparsed)
    stop(errMsg)
  }
  
  as.integer(object)
}

ensure_scalar_double <- function(object) {
  
  if (length(object) != 1 || !is.numeric(object)) {
    deparsed <- deparse(substitute(object))
    errMsg <- sprintf("'%s' is not a length-one numeric value", deparsed)
    stop(errMsg)
  }
  
  as.double(object)
}

ensure_scalar_boolean <- function(object, allow.na = FALSE, default = NULL) {
  if (!is.null(default) && is.null(object)) {
    object = default
  }
  
  if (length(object) != 1) {
    deparsed <- deparse(substitute(object))
    stop(sprintf("'%s' is not a length-one logical value", deparsed))
  }
  
  value <- as.logical(object)
  if (!allow.na && is.na(value)) {
    deparsed <- deparse(substitute(object))
    stop(sprintf("'%s' is NA (must be TRUE/FALSE)", deparsed))
  }
  
  value
}

ensure_scalar_character <- function(object) {
  
  if (length(object) != 1 || !is.character(object)) {
    deparsed <- deparse(substitute(object))
    stop(sprintf("'%s' is not a length-one character vector", deparsed))
  }
  
  as.character(object)
}
