#' @param alpha,lambda Parameters controlling loss function penalization (for e.g.
#'   lasso, elastic net, and ridge regression). See \strong{Details} for more
#'   information.
#'
#' @details
#' Spark implements for both \eqn{L1} and \eqn{L2} regularization in linear
#' regression models. See the preamble in the
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html}{Spark Classification and Regression}
#' documentation for more details on how the loss function is parameterized.
#'
#' In particular, with \code{alpha} set to 1, the parameterization
#' is equivalent to a \href{https://en.wikipedia.org/wiki/Lasso_(statistics)}{lasso}
#' model; if \code{alpha} is set to 0, the parameterization is equivalent to
#' a \href{https://en.wikipedia.org/wiki/Tikhonov_regularization}{ridge regression} model.
