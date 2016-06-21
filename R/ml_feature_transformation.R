#' Register a Spark DataFrame
#' 
#' Registers a Spark DataFrame (giving it a table name for the
#' Spark SQL context), and returns a \code{tbl_spark}.
#' 
#' @param x A Spark DataFrame.
#' @param name A name to assign this table.
#' 
#' @export
sdf_register <- function(x, name = random_string()) {
  spark_invoke(x, "registerTempTable", name)
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
#' @param x A \code{tbl_spark}.
#' @param ... Named parameters, mapping table names to weights. The weights
#'   will be normalized such that they sum to 1.
#' @param weights An alternate mechanism for supplying weights -- when
#'   specified, this takes precedence over the \code{...} arguments.
#' @param seed Random seed to use for randomly partitioning the dataset. Set
#'   this if you want your partitioning to be reproducible on repeated runs.
#'
#' @return An \R \code{list} of \code{tbl_spark}s.
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
  weights <- weights %||% list(...)
  nm <- names(weights)
  if (is.null(nm) || any(!nzchar(nm)))
    stop("all weights must be named")
  partitions <- spark_dataframe_split(x, as.numeric(weights), seed = seed)
  names(partitions) <- nm
  partitions
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

#' Feature Transformation -- VectorAssembler
#'
#' Combine multiple vectors into a single row-vector; that is,
#' where each row element of the newly generated column is a
#' vector formed by concatenating each row element from the
#' specified input columns.
#'
#' @template ml-transformation
#'
#' @export
ft_vector_assembler <- function(x,
                                input_col = NULL,
                                output_col = NULL)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  assembler <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.VectorAssembler"
  )

  transformed <- assembler %>%
    spark_invoke("setInputCols", as.list(input_col)) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- StringIndexer
#'
#' Encode a column of labels into a column of label indices.
#' The indices are in [0, numLabels), ordered by label frequencies, with
#' the most frequent label assigned index 0. The transformation
#' can be reversed with \code{\link{ft_index_to_string}}.
#'
#' @template ml-transformation
#'
#' @param params An (optional) \R environment -- when available,
#'   the index <-> label mapping generated by the string indexer
#'   will be injected into this environment under the \code{labels}
#'   key.
#'
#' @export
ft_string_indexer <- function(x,
                              input_col = NULL,
                              output_col = NULL,
                              params = NULL)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  indexer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.StringIndexer"
  )

  sim <- indexer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("fit", df)

  # Report labels to caller if requested -- these map
  # the discovered labels in the data set to an associated
  # index.
  if (is.environment(params))
    params$labels <- as.character(spark_invoke(sim, "labels"))

  spark_invoke(sim, "transform", df)
}

#' Feature Transformation -- Binarizer
#'
#' Apply thresholding to a column, such that values less than or equal to the
#' \code{threshold} are assigned the value 0.0, and values greater than the
#' threshold are assigned the value 1.0.
#'
#' @template ml-transformation
#'
#' @param threshold The numeric threshold.
#'
#' @export
ft_binarizer <- function(x,
                         input_col = NULL,
                         output_col = NULL,
                         threshold = 0.5)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  binarizer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.Binarizer"
  )

  transformed <- binarizer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("setThreshold", as.double(threshold)) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- Discrete Cosine Transform (DCT)
#'
#' Transform a column in the time domain into another column in the frequency
#' domain.
#'
#' @template ml-transformation
#'
#' @param inverse Perform inverse DCT?
#'
#' @export
ft_discrete_cosine_transform <- function(x,
                                         input_col = NULL,
                                         output_col = NULL,
                                         inverse = FALSE)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  dct <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.DCT"
  )

  transformed <- dct %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("setInverse", as.logical(inverse)) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- IndexToString
#'
#' Symmetrically to \code{\link{ft_string_indexer}},
#' \code{ft_index_to_string} maps a column of label indices back to a
#' column containing the original labels as strings.
#'
#' @template ml-transformation
#'
#' @export
ft_index_to_string <- function(x,
                               input_col = NULL,
                               output_col = NULL)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  converter <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.IndexToString"
  )

  transformed <- converter %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("transform", df)

  transformed
}

## TODO: These routines with so-called 'row vector' features by
## default, but it would be much nicer to implement routines to
## scale whole columns instead.
# ft_standard_scaler <- function(df, input_col, output_col,
#                                      with.mean, with.std)
# {
#   sc <- sparkapi_connection(df)
#
#   scaler <- spark_invoke_new(
#     sc,
#     "org.apache.spark.ml.feature.StandardScaler"
#   )
#
#   scaler %>%
#     spark_invoke("setInputCol", input_col) %>%
#     spark_invoke("setOutputCol", output_col) %>%
#     spark_invoke("setWithMean", as.logical(with.mean)) %>%
#     spark_invoke("setWithStd", as.logical(with.std)) %>%
#     spark_invoke("transform", df)
# }
#
# ft_min_max_scaler <- function(df, input_col, output_col,
#                                     min = 0, max = 1)
# {
#   sc <- sparkapi_connection(df)
#
#   scaler <- spark_invoke_new(
#     sc,
#     "org.apache.spark.ml.feature.MinMaxScaler"
#   )
#
#   scaler %>%
#     spark_invoke("setInputCol", input_col) %>%
#     spark_invoke("setOutputCol", output_col) %>%
#     spark_invoke("setMin", as.numeric(min)) %>%
#     spark_invoke("setMax", as.numeric(max)) %>%
#     spark_invoke("transform", df)
# }

#' Feature Transformation -- Bucketizer
#'
#' Similar to \R's \code{\link{cut}} function, this transforms a numeric column
#' into a discretized column, with breaks specified through the \code{splits}
#' parameter.
#'
#' @template ml-transformation
#'
#' @param splits A numeric vector of cutpoints, indicating the bucket
#'   boundaries.
#'
#' @export
ft_bucketizer <- function(x,
                          input_col = NULL,
                          output_col = NULL,
                          splits)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  bucketizer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.Bucketizer"
  )

  transformed <- bucketizer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("setSplits", as.list(splits)) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- ElementwiseProduct
#'
#' Computes the element-wise product between two columns. Generally, this is
#' intended as a scaling transformation, where an input vector is scaled by
#' another vector, but this should apply for all element-wise product
#' transformations.
#'
#' @template ml-transformation
#'
#' @param scaling_col The column used to scale \code{input_col}.
#'
#' @export
ft_elementwise_product <- function(x,
                                   input_col = NULL,
                                   output_col = NULL,
                                   scaling_col)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  transformer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.ElementwiseProduct"
  )

  transformed <- transformer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("setScalingVec", scaling_col) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- SQLTransformer
#'
#' Transform a data set using SQL. Use the \code{__THIS__}
#' placeholder as a proxy for the active table.
#'
#' @template ml-transformation
#'
#' @param sql A SQL statement.
#'
#' @export
ft_sql_transformer <- function(x,
                               input_col = NULL,
                               output_col = NULL,
                               sql)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  transformer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.SQLTransformer"
  )

  transformed <- transformer %>%
    spark_invoke("setStatement", paste(sql, collapse = "\n")) %>%
    spark_invoke("transform", df)

  transformed
}

#' Feature Transformation -- QuantileDiscretizer
#'
#' Takes a column with continuous features and outputs a column with binned
#' categorical features. The bin ranges are chosen by taking a sample of the
#' data and dividing it into roughly equal parts. The lower and upper bin bounds
#' will be -Infinity and +Infinity, covering all real values. This attempts to
#' find numBuckets partitions based on a sample of the given input data, but it
#' may find fewer depending on the data sample values.
#'
#' Note that the result may be different every time you run it, since the sample
#' strategy behind it is non-deterministic.
#'
#' @template ml-transformation
#'
#' @param n_buckets The number of buckets to use.
#'
#' @export
ft_quantile_discretizer <- function(x,
                                    input_col = NULL,
                                    output_col = NULL,
                                    n_buckets = 5)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)

  discretizer <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.QuantileDiscretizer"
  )

  transformed <- discretizer %>%
    spark_invoke("setInputCol", input_col) %>%
    spark_invoke("setOutputCol", output_col) %>%
    spark_invoke("setNumBuckets", as.numeric(n_buckets)) %>%
    spark_invoke("fit", df) %>%
    spark_invoke("transform", df)

  transformed
}
