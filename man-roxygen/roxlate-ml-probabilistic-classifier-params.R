#' @param probability_col Column name for predicted class conditional probabilities.
#' @param raw_prediction_col Raw prediction (a.k.a. confidence) column name.
#' @param thresholds Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value \code{p/t} is predicted, where \code{p} is the original probability of that class and \code{t} is the class's threshold.
