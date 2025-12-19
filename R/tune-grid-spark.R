#' Model tuning via grid search inside Spark
#' @param sc A Spark Connection
#' @param object A parsnip model specification or an unfitted `workflow()`. No
#' tuning parameters are allowed; if arguments have been marked with `tune()`,
#' their values must be finalized.
#' @param ...	Not currently used.
#' @param preprocessor A traditional model formula or a recipe created using
#' `recipes::recipe().`
#' @param resamples	An rset resampling object created from an rsample function,
#' such as `rsample::vfold_cv()`.
#' @param param_info A `dials::parameters()` object or NULL. If none is given,
#' a parameters set is derived from other arguments. Passing this argument can
#'  be useful when parameter ranges need to be customized.
#' @param grid A data frame of tuning combinations or a positive integer. The
#' data frame should have columns for each parameter being tuned and rows for
#'  tuning parameter candidates. An integer denotes the number of candidate
#'   parameter sets to be created automatically.
#' @param metrics A `yardstick::metric_set()`, or NULL to compute a standard
#' set of metrics.
#' @param eval_time A numeric vector of time points where dynamic event time
#'  metrics should be computed (e.g. the time-dependent ROC curve, etc). The
#'  values must be non-negative and should probably be no greater than the
#'  largest event time in the training set (See Details below).
#' @param control	An object used to modify the tuning process, likely created
#' by `control_grid()`.
#' @param no_tasks Number of parallel tasks (jobs) to request Spark
#'
#' @export
tune_grid_spark <- function(
    sc,
    object,
    preprocessor,
    resamples,
    ...,
    param_info = NULL,
    grid = 10,
    metrics = NULL,
    eval_time = NULL,
    control = tune::control_grid(),
    no_tasks = NULL
) {
  UseMethod("tune_grid_spark")
}
