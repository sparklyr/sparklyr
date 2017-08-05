#' @param col.sample.rate The sampling rate of features to consider for splits at each tree node.
#'  Defaults to 1/3 for regression and sqrt(k)/k for classification where k is number of features.
#'  For Spark versions prior to 2.0.0, arbitrary sampling rates are not supported, so the input is
#'  automatically mapped to one of "onethird", "sqrt", or "log2".
