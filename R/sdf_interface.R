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
#' @export
sdf_register <- function(x, name) {
  UseMethod("sdf_register")
}

#' @export
sdf_register.list <- function(x, name = names(x)) {
  result <- lapply(seq_along(x), function(i) {
    sdf_register(x[[i]], name[[i]])
  })
  names(result) <- name
  result
}

#' @export
sdf_register.sparkapi_jobj <- function(x, name = random_string()) {
  sparkapi_invoke(x, "registerTempTable", name)
  on_connection_updated(sparkapi_connection(x), name)
  tbl(sparkapi_connection(x), name)
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
#' @family Spark DataFrame functions
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
  names(partitions) <- nm
  partitions
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
#' @family Spark DataFrame functions
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
  
  sampled
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
#' @family Spark DataFrame functions
#' 
#' @export
sdf_sort <- function(x, columns) {
  df <- sparkapi_dataframe(x)
  
  columns <- as.character(columns)
  n <- length(columns)
  if (n == 0)
    stop("must supply one or more column names")
  
  if (n == 1) {
    sparkapi_invoke(df, "sort", columns, list())
  } else {
    sparkapi_invoke(df, "sort", columns[[1]], as.list(columns[-1]))
  }
  
}

#' Mutate a Spark DataFrame
#'
#' Use Spark's \href{http://spark.apache.org/docs/latest/ml-features.html}{feature transformers}
#' to mutate a Spark DataFrame.
#' 
#' \code{sdf_mutate()} differs from \code{mutate} in a number of important ways:
#' 
#' \itemize{
#' 
#' \item \code{mutate} returns a \code{tbl_spark}, while \code{sdf_mutate} returns
#'   a Spark DataFrame (represented by a \code{jobj}),
#'   
#' \item \code{mutate} works 'lazily' (the generated SQL is not evaluated until \code{collect}
#'   is called), while \code{sdf_mutate} works 'eagerly' (the feature transformer, as well as
#'   and pending SQL from a previous pipeline, is applied),
#'   
#' \item To transform the Spark DataFrame back to a \code{tbl_spark}, you should
#'   use \code{\link{sdf_register}}.
#'  
#' }
#' 
#' Overall, this implies that if you wish to mix a \code{dplyr} pipeline with \code{sdf_mutate},
#' you should generally apply your \code{dplyr} pipeline first, then finalize your output with
#' \code{sdf_mutate}. See \strong{Examples} for an example of how this might be done.
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
  data
}
