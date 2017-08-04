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

#' Project features onto principal components
#'
#' @template roxlate-sdf
#' @param object A Spark PCA model object
#' @param newdata An object coercible to a Spark DataFrame
#' @param features A vector of names of columns to be projected
#' @param feature.prefix The prefix used in naming the output features
#' @param ... Optional arguments; currently unused.
#'
#' @export
sdf_project <- function(object, newdata,
                        features = dimnames(object$components)[[1]],
                        feature.prefix = "PC", ...) {

  ensure_scalar_character(feature.prefix)

  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$data

  id.column <- object$ml.options$id.column
  features.column <- object$ml.options$features.column
  output.column <- object$ml.options$output.column

  sdf <- ml_prepare_dataframe(newdata,
                       features = features,
                       ml.options = ml_options(features.column = features.column)
  )

  object$.model %>%
    invoke("transform", sdf) %>%
    invoke("drop", id.column) %>%
    invoke("drop", features.column) %>%
    sdf_register() %>%
    sdf_separate_column(output.column,
                        into = paste0(feature.prefix, seq_len(object$k))) %>%
    select(-!!rlang::sym(output.column))
}
