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
#' @param name The name to assign to the copied table in Spark.
#' @param memory Boolean; should the table be cached into memory?
#' @param repartition The number of partitions to use when distributing the
#'   table across the Spark cluster. The default (0) can be used to avoid
#'   partitioning.
#' @param overwrite Boolean; overwrite a pre-existing table with the name \code{name}
#'   if one already exists?
#' @param ... Optional arguments, passed to implementing methods.
#'
#' @family Spark data frames
#'
#' @examples
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' sdf_copy_to(sc, iris)
#'
#' @name sdf_copy_to
#' @export
sdf_copy_to <- function(sc,
                        x,
                        name,
                        memory,
                        repartition,
                        overwrite,
                        ...) {
  UseMethod("sdf_copy_to")
}

#' @export
sdf_copy_to.default <- function(sc,
                                x,
                                name = deparse(substitute(x)),
                                memory = TRUE,
                                repartition = 0L,
                                overwrite = FALSE,
                                ...
) {
  sdf_import(x, sc, name, memory, repartition, overwrite, ...)
}

#' @name sdf_copy_to
#' @export
sdf_import <- function(x,
                       sc,
                       name,
                       memory,
                       repartition,
                       overwrite,
                       ...) {
  UseMethod("sdf_import")
}

#' @export
sdf_import.default <- function(x,
                               sc,
                               name = random_string("sparklyr_tmp_"),
                               memory = TRUE,
                               repartition = 0L,
                               overwrite = FALSE,
                               ...)
{
  # ensure data.frame
  if (!is.data.frame(x)) {
    x <- as.data.frame(
      x,
      stringsAsFactors = FALSE,
      row.names = FALSE,
      optional = TRUE
    )
  }

  if (overwrite)
    spark_remove_table_if_exists(sc, name)
  else if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dots <- list(...)
  serializer <- dots$serializer
  spark_data_copy(sc, x, name = name, repartition = repartition, serializer = serializer)

  if (memory)
    tbl_cache(sc, name)

  on_connection_updated(sc, name)

  tbl(sc, name)
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
sdf_register.tbl_spark <- function(x, name = NULL) {
  sdf_register(spark_dataframe(x), name)
}

#' @export
sdf_register.list <- function(x, name = NULL) {
  result <- lapply(seq_along(x), function(i) {
    sdf_register(x[[i]], name[[i]])
  })
  names(result) <- name
  result
}

#' @export
sdf_register.spark_jobj <- function(x, name = NULL) {
  name <- name %||% random_string("sparklyr_tmp_")
  invoke(x, "registerTempTable", name)
  sc <- spark_connection(x)
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
  sdf <- spark_dataframe(x)
  weights <- weights %||% list(...)
  nm <- names(weights)
  if (is.null(nm) || any(!nzchar(nm)))
    stop("all weights must be named")
  partitions <- sdf_split(sdf, as.numeric(weights), seed = seed)
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
  sdf <- spark_dataframe(x)

  sampled <- if (is.null(seed)) {
    sdf %>%
      invoke("sample", as.logical(replacement), as.double(fraction))
  } else {
    sdf %>%
      invoke("sample", as.logical(replacement), as.double(fraction), as.integer(seed))
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
  df <- spark_dataframe(x)

  columns <- as.character(columns)
  n <- length(columns)
  if (n == 0)
    stop("must supply one or more column names")

  sorted <- if (n == 1) {
    invoke(df, "sort", columns, list())
  } else {
    invoke(df, "sort", columns[[1]], as.list(columns[-1]))
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
#' DataFrame with predicted values encoded in the \code{"prediction"} column.
#'
#' @param object,newdata An object coercable to a Spark DataFrame.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark data frames
#'
#' @export
sdf_predict <- function(object, newdata, ...) {

  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$data

  # convert to spark dataframe
  sdf <- spark_dataframe(newdata)

  # construct dummy variables if required
  ctfm <- object$categorical.transformations
  if (is.environment(ctfm)) {
    ctfm <- as.list(ctfm)
    enumerate(ctfm, function(key, val) {
      sdf <<- ml_create_dummy_variables(
        x = sdf,
        input = key,
        reference = val$reference,
        levels = val$levels,
        labels = val$labels
      )
    })
  }

  # assemble into feature vector, apply transformation and return predictions
  params <- object$model.parameters
  assembled <- spark_dataframe(ft_vector_assembler(sdf, object$features, params$features))
  transformed <- invoke(object$.model, "transform", assembled)
  dropped <- invoke(transformed, "drop", params$features)
  sdf_register(dropped)
}

#' Create Dummy Variables
#'
#' Given a column in a Spark DataFrame, generate a new Spark DataFrame
#' containing dummy variable columns.
#'
#' The dummy variables are generated in a similar mechanism to
#' \code{\link{model.matrix}}, where categorical variables are expanded into a
#' set of binary (dummy) variables. These dummy variables can be used for
#' regression of categorical variables within the various regression routines
#' provided by \code{sparklyr}.
#'
#' @section Auxiliary Information:
#'
#' The \code{envir} argument can be used as a mechanism for returning
#' optional information. Currently, the following pieces are returned:
#'
#' \tabular{ll}{
#' \code{levels}:\tab The set of unique values discovered within the input column.\cr
#' \code{columns}:\tab The column names generated.\cr
#' }
#'
#' If the \code{envir} argument is supplied, the names of any dummy variables
#' generated will be included, under the \code{labels} key.
#'
#' @template roxlate-ml-x
#' @param input The name of the input column.
#' @param reference The reference label. This variable is omitted when
#'   generating dummy variables (to avoid perfect multi-collinearity if
#'   all dummy variables were to be used in the model fit); to generate
#'   dummy variables for all columns this can be explicitly set as \code{NULL}.
#' @param levels The set of levels for which dummy variables should be generated.
#'   By default, constructs one variable for each unique value occurring in
#'   the column specified by \code{input}.
#' @param labels An optional \R list, mapping values in the \code{input}
#'   column to column names to be assigned to the associated dummy variable.
#' @param envir An optional \R environment; when provided, it will be filled
#'   with useful auxiliary information. See \strong{Auxiliary Information} for
#'   more information.
#'
#' @export
ml_create_dummy_variables <- function(x,
                                      input,
                                      reference = NULL,
                                      levels = NULL,
                                      labels = NULL,
                                      envir = new.env(parent = emptyenv()))
{
  sdf <- spark_dataframe(x)

  # validate inputs
  input <- ensure_scalar_character(input)
  reference <- if (!is.null(reference)) ensure_scalar_character(reference)

  # read unique values (levels) for our categorical variable
  counts <- sdf %>%
    invoke("select", input, list()) %>%
    invoke("groupBy", input, list()) %>%
    invoke("count") %>%
    invoke("orderBy", "count", list()) %>%
    sdf_collect()

  # validate that 'reference' is a valid label for this column
  levels <- levels %||% sort(counts[[input]])
  if (!is.null(reference) && !reference %in% levels) {
    fmt <- "no label called '%s' in column '%s'; valid labels are:\n- %s\n"
    msg <- sprintf(fmt, reference, input, paste(shQuote(levels), collapse = ", "))
    stop(msg, call. = FALSE)
  }

  # extract columns of dataset (as Spark Column objects)
  colNames  <- invoke(sdf, "columns")
  colValues <- lapply(colNames, function(colName) {
    invoke(sdf, "col", colName)
  })

  # generate dummy variables for each level
  dummyValues <- lapply(levels, function(level) {
    sdf %>%
      invoke("col", input) %>%
      invoke("equalTo", level) %>%
      invoke("cast", "double")
  })

  # generate appropriate names for the columns
  if (is.null(labels)) {
    labels <- lapply(levels, function(level) {
      paste(input, level, sep = "_")
    })
    names(labels) <- levels
  }

  dummyNames <- unlist(unname(labels))

  # extract a new Spark DataFrame with these columns
  mutated <- sdf %>%
    invoke("select", c(colValues, dummyValues)) %>%
    invoke("toDF", c(colNames, dummyNames))

  # report useful information in output env
  if (is.environment(envir)) {
    envir$levels    <- levels
    envir$labels    <- labels
    envir$reference <- reference
    envir$columns   <- dummyNames
    envir$counts    <- counts
  }

  # return our new table
  sdf_register(mutated)
}

#' Add a Unique ID Column to a Spark DataFrame
#'
#' Add a unique ID column to a Spark DataFrame. The Spark
#' \code{monotonicallyIncreasingId} function is used to produce these and is
#' guaranteed to produce unique, monotonically increasing ids; however, there
#' is no guarantee that these IDs will be sequential. The table is persisted
#' immediately after the column is generated, to ensure that the column is
#' stable -- otherwise, it can differ across new computations.
#'
#' @template roxlate-ml-x
#' @param id The name of the column to host the generated IDs.
#'
#' @export
sdf_with_unique_id <- function(x, id = "id") {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  ensure_scalar_character(id)

  mii <- invoke_static(
    sc,
    "org.apache.spark.sql.functions",
    "monotonicallyIncreasingId"
  )

  mii <- invoke(mii, "cast", "double")

  transformed <- sdf %>%
    invoke("withColumn", id, mii) %>%
    sdf_persist(storage.level = "MEMORY_ONLY")

  sdf_register(transformed)
}

#' Compute (Approximate) Quantiles with a Spark DataFrame
#'
#' Given a numeric column within a Spark DataFrame, compute
#' approximate quantiles (to some relative error).
#'
#' @template roxlate-ml-x
#' @param column The column for which quantiles should be computed.
#' @param probabilities A numeric vector of probabilities, for
#'   which quantiles should be computed.
#' @param relative.error The relative error -- lower values imply more
#'   precision in the computed quantiles.
#'
#' @export
sdf_quantile <- function(x,
                         column,
                         probabilities = c(0.00, 0.25, 0.50, 0.75, 1.00),
                         relative.error = 1E-5)
{
  sdf <- spark_dataframe(x)

  nm <-
    names(probabilities) %||%
    paste(signif(probabilities * 100, 3), "%", sep = "")

  column <- ensure_scalar_character(column)
  probabilities <- as.list(as.numeric(probabilities))
  relative.error <- ensure_scalar_double(relative.error)

  stat <- invoke(sdf, "stat")
  quantiles <- invoke(stat, "approxQuantile", column, probabilities, relative.error)
  names(quantiles) <- nm

  quantiles
}

#' Persist a Spark DataFrame
#'
#' Persist a Spark DataFrame, forcing any pending computations and (optionally)
#' serializing the results to disk.
#'
#' Spark DataFrames invoke their operations lazily -- pending operations are
#' deferred until their results are actually needed. Persisting a Spark
#' DataFrame effectively 'forces' any pending computations, and then persists
#' the generated Spark DataFrame as requested (to memory, to disk, or
#' otherwise).
#'
#' Users of Spark should be careful to persist the results of any computations
#' which are non-deterministic -- otherwise, one might see that the values
#' within a column seem to 'change' as new operations are performed on that
#' data set.
#'
#' @template roxlate-ml-x
#' @param storage.level The storage level to be used. Please view the
#'   \href{http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence}{Spark Documentation}
#'   for information on what storage levels are accepted.
#' @export
sdf_persist <- function(x, storage.level = "MEMORY_AND_DISK") {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  storage.level <- ensure_scalar_character(storage.level)

  sl <- invoke_static(
    sc,
    "org.apache.spark.storage.StorageLevel",
    storage.level
  )

  sdf %>%
    invoke("persist", sl) %>%
    sdf_register()
}
