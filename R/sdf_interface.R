#' Copy an Object into Spark
#' 
#' Copy an object into Spark, and return an \R object wrapping the
#' copied object (typically, a Spark DataFrame).
#' 
#' @section Advanced Usage:
#' 
#' \code{sdf_copy_to} is an S3 generic that, by default, dispatches to
#' \code{sdf_import}. Package authors that would like to implement
#' \code{sdf_copy_to} for a custom object type can accomplish this by
#' implementing the associated method on \code{sdf_import}.
#' 
#' @param sc The associated Spark connection.
#' @param x An \R object from which a Spark DataFrame can be generated.
#' @param ... Optional arguments, passed to implementing methods.
#' 
#' @family Spark data frames
#' 
#' @name sdf_copy_to
#' @export
sdf_copy_to <- function(sc, x, ...) {
  UseMethod("sdf_copy_to")
}

#' @export
sdf_copy_to.default <- function(sc, x, ...) {
  sdf_import(x, sc)
}

#' @name sdf_copy_to
#' @export
sdf_import <- function(x, sc, ...) {
  UseMethod("sdf_import")
}

#' @export
sdf_import.default <- function(x, sc, ..., name = NULL, cache = TRUE) {
  
  # ensure data.frame
  if (!is.data.frame(x)) {
    x <- as.data.frame(
      x,
      stringsAsFactors = FALSE,
      row.names = FALSE,
      optional = TRUE
    )
  }
  
  # generate a CSV file from the associated data frame
  # note that these files need to live for the R session
  # duration so we don't clean these up eagerly
  # write file based on hash to avoid writing too many files
  # on repeated import calls
  hash <- digest::digest(x, algo = "sha256")
  filename <- paste("spark_csv_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(), filename)
  
  if (!file.exists(tempfile)) {
    readr::write_csv(
      x,
      path = tempfile,
      col_names = TRUE
    )
  }
  
  # generate path that Spark can use
  path <- normalizePath(tempfile, winslash = "/", mustWork = TRUE)
  
  # generate the Spark CSV reader
  ctx <- spark_api_create_sql_context(sc)
  reader <- sparkapi_invoke(ctx, "read")
  
  # construct schema
  # TODO: move to separate function?
  fields <- lapply(names(x), function(name) {
    
    # infer the type
    value <- x[[name]]
    type <- if (is.factor(value))
      "character"
    else
      typeof(value)
    
    # create struct field
    sparkapi_invoke_static(
      sc,
      
      "org.apache.spark.sql.api.r.SQLUtils",
      "createStructField",
      
      name,
      type,
      TRUE
      
    )
  })
  
  schema <- sparkapi_invoke_static(
    sc,
    
    "org.apache.spark.sql.api.r.SQLUtils",
    "createStructType",
    
    fields
  )
  
  # invoke CSV reader with our schema
  sdf <- reader %>%
    sparkapi_invoke("format", "com.databricks.spark.csv") %>%
    sparkapi_invoke("option", "header", "true") %>%
    sparkapi_invoke("schema", schema) %>%
    sparkapi_invoke("load", path)
  
  if (cache)
    sdf <- sparkapi_invoke(sdf, "cache")
  
  sdf_register(sdf, name)
}

#' Register a Spark DataFrame
#' 
#' Registers a Spark DataFrame (giving it a table name for the
#' Spark SQL context), and returns a \code{tbl_spark}.
#' 
#' @template roxlate-sdf
#' 
#' @param x A Spark DataFrame.
#' @param name A name to assign this table.
#' 
#' @family Spark data frames
#' @export
sdf_register <- function(x, name = NULL) {
  UseMethod("sdf_register")
}

#' @export
sdf_register.list <- function(x, name = NULL) {
  if (is.null(name))
    name <- replicate(length(x), random_string("sparklyr_tmp_"))
  result <- lapply(seq_along(x), function(i) {
    sdf_register(x[[i]], name[[i]])
  })
  names(result) <- name
  result
}

#' @export
sdf_register.sparkapi_jobj <- function(x, name = random_string("sparklyr_tmp_")) {
  sparkapi_invoke(x, "registerTempTable", name)
  sc <- sparkapi_connection(x)
  on_connection_updated(sc, name)
  tbl(sc, name)
}

#' Partition a Spark Dataframe
#'
#' Partition a Spark DataFrame into multiple groups. This routine is useful
#' for splitting a DataFrame into, for example, training and test datasets.
#' 
#' The sampling weights define the probability that a particular observation
#' will be assigned to a particular partition, not the resulting size of the
#' partition. This implies that partitioning a DataFrame with, for example,
#' 
#' \code{sdf_partition(x, training = 0.5, test = 0.5)}
#' 
#' is not guaranteed to produce \code{training} and \code{test} partitions
#' of equal size.
#' 
#' @template roxlate-sdf
#'
#' @param x An object coercable to a Spark DataFrame.
#' @param ... Named parameters, mapping table names to weights. The weights
#'   will be normalized such that they sum to 1.
#' @param weights An alternate mechanism for supplying weights -- when
#'   specified, this takes precedence over the \code{...} arguments.
#' @param seed Random seed to use for randomly partitioning the dataset. Set
#'   this if you want your partitioning to be reproducible on repeated runs.
#'
#' @return An \R \code{list} of \code{tbl_spark}s.
#' 
#' @family Spark data frames
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # randomly partition data into a 'training' and 'test'
#' # dataset, with 60% of the observations assigned to the
#' # 'training' dataset, and 40% assigned to the 'test' dataset
#' data(diamonds, package = "ggplot2")
#' diamonds_tbl <- copy_to(sc, diamonds, "diamonds")
#' partitions <- diamonds_tbl %>%
#'   sdf_partition(training = 0.6, test = 0.4)
#' print(partitions)
#' 
#' # alternate way of specifying weights
#' weights <- c(training = 0.6, test = 0.4)
#' diamonds_tbl %>% sdf_partition(weights = weights)
#' }
sdf_partition <- function(x,
                          ...,
                          weights = NULL,
                          seed = sample(.Machine$integer.max, 1))
{
  sdf <- sparkapi_dataframe(x)
  weights <- weights %||% list(...)
  nm <- names(weights)
  if (is.null(nm) || any(!nzchar(nm)))
    stop("all weights must be named")
  partitions <- spark_dataframe_split(sdf, as.numeric(weights), seed = seed)
  registered <- sdf_register(partitions)
  names(registered) <- nm
  registered
}

#' Randomly Sample Rows from a Spark DataFrame
#' 
#' Draw a random sample of rows (with or without replacement)
#' from a Spark DataFrame.
#' 
#' @template roxlate-sdf
#' 
#' @param x An object coercable to a Spark DataFrame.
#' @param fraction The fraction to sample.
#' @param replacement Boolean; sample with replacement?
#' @param seed An (optional) integer seed.
#' 
#' @family Spark data frames
#' 
#' @export
sdf_sample <- function(x, fraction = 1, replacement = TRUE, seed = NULL)
{
  sdf <- sparkapi_dataframe(x)
  
  sampled <- if (is.null(seed)) {
    sdf %>%
      sparkapi_invoke("sample", as.logical(replacement), as.double(fraction))
  } else {
    sdf %>%
      sparkapi_invoke("sample", as.logical(replacement), as.double(fraction), as.integer(seed))
  }
  
  sdf_register(sampled)
}

#' Sort a Spark DataFrame
#' 
#' Sort a Spark DataFrame by one or more columns, with each column
#' sorted in ascending order.
#' 
#' @template roxlate-sdf
#' 
#' @param x An object coercable to a Spark DataFrame.
#' @param columns The column(s) to sort by.
#' 
#' @family Spark data frames
#' 
#' @export
sdf_sort <- function(x, columns) {
  df <- sparkapi_dataframe(x)
  
  columns <- as.character(columns)
  n <- length(columns)
  if (n == 0)
    stop("must supply one or more column names")
  
  sorted <- if (n == 1) {
    sparkapi_invoke(df, "sort", columns, list())
  } else {
    sparkapi_invoke(df, "sort", columns[[1]], as.list(columns[-1]))
  }
  
  sdf_register(sorted)
}

#' Mutate a Spark DataFrame
#'
#' Use Spark's \href{http://spark.apache.org/docs/latest/ml-features.html}{feature transformers}
#' to mutate a Spark DataFrame.
#' 
#' @template roxlate-sdf
#'
#' @param .data A \code{spark_tbl}.
#' @param ... Named arguments, mapping new column names to the transformation to
#'   be applied.
#' @param .dots A named list, mapping output names to transformations.
#'
#' @name sdf_mutate
#' @export
#'
#' @family feature transformation routines
#'
#' @examples
#' \dontrun{
#' # using the 'beaver1' dataset, binarize the 'temp' column
#' data(beavers, package = "datasets")
#' beaver_tbl <- copy_to(sc, beaver1, "beaver")
#' beaver_tbl %>%
#'   mutate(squared = temp ^ 2) %>%
#'   sdf_mutate(warm = ft_binarizer(squared, 1000)) %>%
#'   sdf_register("mutated")
#' 
#' # view our newly constructed tbl
#' head(beaver_tbl)
#' 
#' # note that we have two separate tbls registered
#' dplyr::src_tbls(sc)
#' }
sdf_mutate <- function(.data, ...) {
  sdf_mutate_(.data, .dots = lazyeval::lazy_dots(...))
}

#' @name sdf_mutate
#' @export
sdf_mutate_ <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  data <- .data
  
  for (i in seq_along(dots)) {
    
    # extract expression to be evaluated
    lazy_expr <- dots[[i]]$expr
    lazy_env  <- dots[[i]]$env
    
    # figure out the input column -- we aren't being very
    # principled about non-standard evaluation here
    el <- lazy_expr[[2]]
    input_col <- if (is.call(el)) {
      eval(el, envir = lazy_env)
    } else {
      as.character(el)
    }
    
    output_col <- as.character(names(dots)[[i]])
    
    # construct a new call with the input variable injected
    # for evaluation
    preamble <- list(
      lazy_expr[[1]], # function
      data,           # data
      input_col,      # input column
      output_col      # output column
    )
    
    call <- as.call(c(
      preamble,
      as.list(lazy_expr[-c(1, 2)])
    ))
    
    # evaluate call
    data <- eval(call, envir = lazy_env)
  }
  
  # return mutated dataset
  sdf_register(data)
}

#' Model Predictions with Spark DataFrames
#' 
#' Given a \code{ml_model} fit alongside a new data set, produce a new Spark
#' DataFrame with predicted values encoded in the \code{prediction} column.
#' 
#' @param object,newdata An object coercable to a Spark DataFrame.
#' @param prediction The name to assign to the column hosting the predicted values.
#' @param ... Optional arguments; currently unused.
#' 
#' @family Spark data frames
#' 
#' @export
sdf_predict <- function(object, newdata, prediction = "prediction", ...) {
  prediction <- ensure_scalar_character(prediction)
  if (missing(newdata) || is.null(newdata))
    newdata <- object$data
  sdf <- sparkapi_dataframe(newdata)
  params <- object$model.parameters
  assembled <- ft_vector_assembler(sdf, object$features, params$features)
  model <- object$.model
  model <- sparkapi_invoke(model, "setPredictionCol", prediction)
  transformed <- sparkapi_invoke(model, "transform", assembled)
  dropped <- sparkapi_invoke(transformed, "drop", params$features)
  sdf_register(dropped)
}
