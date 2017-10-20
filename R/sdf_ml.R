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
#' @param feature_prefix The prefix used in naming the output features
#' @param ... Optional arguments; currently unused.
#'
#' @export
sdf_project <- function(object, newdata,
                        features = dimnames(object$pc)[[1]],
                        feature_prefix = NULL, ...) {

  dots <- list(...)
  if (!rlang::is_null(dots$feature.prefix))
    assign("feature_prefix", dots$feature.prefix)


  ensure_scalar_character(feature_prefix, allow.null = TRUE)

  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  output_names <- if (rlang::is_null(feature_prefix))
    dimnames(object$pc)[[2]]
  else
    paste0(feature_prefix, seq_len(object$k))

  assembled <- random_string("assembled")
  out <- random_string("out")

  object$model %>%
    ml_set_param("input_col", assembled) %>%
    ml_set_param("output_col", out) %>%
    ml_transform(ft_vector_assembler(newdata, features, assembled)) %>%
    sdf_separate_column(column = out, into = output_names) %>%
    select(!!! rlang::syms(c(colnames(newdata), output_names)))
}
