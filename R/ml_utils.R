#' Prepare a Spark DataFrame for Spark ML Routines
#'
#' This routine prepares a Spark DataFrame for use by Spark ML routines.
#'
#' Spark DataFrames are prepared through the following transformations:
#'
#' \enumerate{
#'   \item All specified columns are transformed into a numeric data type
#'         (using a simple cast for integer / logical columns, and
#'         \code{\link{ft_string_indexer}} for strings),
#'   \item The \code{\link{ft_vector_assembler}} is used to combine the
#'         specified features into a single 'feature' vector, suitable
#'         for use with Spark ML routines.
#' }
#'
#' After calling this function, the \code{envir} environment (when supplied)
#' will be populated with a set of variables:
#'
#' \tabular{ll}{
#' \code{features}:\tab The name of the generated \code{features} vector.\cr
#' \code{response}:\tab The name of the generated \code{response} vector.\cr
#' \code{labels}:  \tab When the \code{response} column is a string vector,
#'                      the \code{\link{ft_string_indexer}} is used to transform
#'                      the vector into a [0:n) numeric vector. The ordered
#'                      labels are injected here to allow for easier mapping
#'                      from the [0:n) values back to the original label.
#' }
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-features
#' @template roxlate-ml-response
#' @template roxlate-ml-dots
#' @param envir An \R environment -- when supplied, it will be filled
#'   with metadata describing the transformations that have taken place.
#'
#' @export
#' @examples
#' \dontrun{
#' # example of how 'ml_prepare_dataframe' might be used to invoke
#' # Spark's LinearRegression routine from the 'ml' package
#' envir <- new.env(parent = emptyenv())
#' tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
#'
#' lr <- invoke_new(
#'   sc,
#'   "org.apache.spark.ml.regression.LinearRegression"
#' )
#'
#' # use generated 'features', 'response' vector names in model fit
#' model <- lr %>%
#'   invoke("setFeaturesCol", envir$features) %>%
#'   invoke("setLabelCol", envir$response)
#' }
ml_prepare_dataframe <- function(x, features, response = NULL, ...,
                                 envir = new.env(parent = emptyenv()))
{
  df <- spark_dataframe(x)
  schema <- sdf_schema(df)

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
        invoke("col", response) %>%
        invoke("cast", "double")
      df <- df %>%
        invoke("withColumn", envir$response, castedColumn)
    }
  }

  # assemble features vector and return
  transformed <- ft_vector_assembler(df, features, envir$features)

  # return as vanilla spark dataframe
  spark_dataframe(transformed)
}

try_null <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' @export
predict.ml_model <- function(object, newdata, ...) {
  # 'sdf_predict()' does not necessarily return a data set with the same row
  # order as the input data; generate a unique id and re-join the generated
  # spark dataframe to ensure the row order is maintained
  id <- random_string("id_")
  sdf <- newdata %>%
    sdf_with_unique_id(id) %>%
    spark_dataframe()

  # perform prediction
  params <- object$model.parameters
  predicted <- sdf_predict(object, sdf, ...)

  # join prediction column on original data, then read prediction column
  column <- sdf %>%
    invoke("join", spark_dataframe(predicted), as.list(id)) %>%
    sdf_read_column("prediction")

  # re-order based on id
  if (is.character(params$labels) && is.numeric(column))
    column <- params$labels[column + 1]

  column
}

#' @export
fitted.ml_model <- function(object, ...) {

  predictions <- object$.model %>%
    invoke("summary") %>%
    invoke("predictions")

  id <- object$model.parameters$id
  object$data %>%
    invoke("join", predictions, as.list(id)) %>%
    sdf_read_column("prediction")
}

#' @export
residuals.ml_model <- function(object, ...) {
  object$.model %>%
    invoke("summary") %>%
    invoke("residuals") %>%
    sdf_read_column("residuals")
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
  object <- invoke(jobj, field)
  invoke(object, "toArray")
}

read_spark_matrix <- function(jobj, field) {
  object <- invoke(jobj, field)
  nrow <- invoke(object, "numRows")
  ncol <- invoke(object, "numCols")
  data <- invoke(object, "toArray")
  matrix(data, nrow = nrow, ncol = ncol)
}

ensure_not_na <- function(object) {
  if (any(is.na(object))) {
    stopf(
      "'%s' %s",
      deparse(substitute(object)),
      if (length(object) > 1) "contains NA values" else "is NA"
    )
  }

  object
}

ensure_not_null <- function(object) {
  object %||% stop(sprintf("'%s' is NULL", deparse(substitute(object))))
}

ensure_scalar <- function(object) {

  if (length(object) != 1 || !is.numeric(object)) {
    stopf(
      "'%s' is not a length-one numeric value",
      deparse(substitute(object))
    )
  }

  object
}

#' Enforce Specific Structure for R Objects
#'
#' These routines are useful when preparing to pass objects to
#' a Spark routine, as it is often necessary to ensure certain
#' parameters are scalar integers, or scalar doubles, and so on.
#'
#' @param object An \R object.
#' @param allow.na Are \code{NA} values permitted for this object?
#' @param allow.null Are \code{NULL} values permitted for this object?
#' @param default If \code{object} is \code{NULL}, what value should
#'   be used in its place? If \code{default} is specified, \code{allow.null}
#'   is ignored (and assumed to be \code{TRUE}).
#'
#' @name ensure
#' @rdname ensure
NULL

make_ensure_scalar_impl <- function(checker,
                                    message,
                                    converter)
{
  fn <- function(object,
                 allow.na = FALSE,
                 allow.null = FALSE,
                 default = NULL)
  {
    object <- object %||% default

    if (!checker(object))
      stopf("'%s' is not %s", deparse(substitute(object)), message)

    if (is.na(object)) object <- NA_integer_
    if (!allow.na)     ensure_not_na(object)
    if (!allow.null)   ensure_not_null(object)

    converter(object)
  }

  environment(fn) <- parent.frame()

  body(fn) <- do.call(
    substitute,
    list(
      body(fn),
      list(
        checker = substitute(checker),
        message = substitute(message),
        converter = substitute(converter)
      )
    )
  )

  fn
}

#' @rdname ensure
#' @name ensure
#' @export
ensure_scalar_integer <- make_ensure_scalar_impl(
  is.numeric,
  "a length-one integer vector",
  as.integer
)

#' @rdname ensure
#' @name ensure
#' @export
ensure_scalar_double <- make_ensure_scalar_impl(
  is.numeric,
  "a length-one numeric vector",
  as.double
)

#' @rdname ensure
#' @name ensure
#' @export
ensure_scalar_boolean <- make_ensure_scalar_impl(
  is.logical,
  "a length-one logical vector",
  as.logical
)

#' @rdname ensure
#' @name ensure
#' @export
ensure_scalar_character <- make_ensure_scalar_impl(
  is.character,
  "a length-one character vector",
  as.character
)
