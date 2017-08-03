#' @param thresholds Thresholds in multi-class classification to adjust
#' the probability of predicting each class. Vector must have length equal
#' to the number of classes, with values > 0 excepting that at most one
#' value may be 0. The class with largest value p/t is predicted, where p
#' is the original probability of that class and t is the class's threshold.
