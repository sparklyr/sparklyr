#' @param feature_subset_strategy The number of features to consider for splits at each tree node. See details for options.
#' @details The supported options for \code{feature_subset_strategy} are
#'   \itemize{
#'     \item \code{"auto"}: Choose automatically for task: If \code{num_trees == 1}, set to \code{"all"}. If \code{num_trees > 1} (forest), set to \code{"sqrt"} for classification and to \code{"onethird"} for regression.
#'     \item \code{"all"}: use all features
#'     \item \code{"onethird"}: use 1/3 of the features
#'     \item \code{"sqrt"}: use use sqrt(number of features)
#'     \item \code{"log2"}: use log2(number of features)
#'     \item \code{"n"}: when \code{n} is in the range (0, 1.0], use n * number of features. When \code{n} is in the range (1, number of features), use \code{n} features. (default = \code{"auto"})
#'     }
