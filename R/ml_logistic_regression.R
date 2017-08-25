#' #' Spark ML -- Logistic Regression
#' #'
#' #' Perform logistic regression on a Spark DataFrame.
#' #'
#' #' @template roxlate-ml-x
#' #' @template roxlate-ml-response
#' #' @template roxlate-ml-features
#' #' @template roxlate-ml-intercept
#' #' @template roxlate-ml-regression-penalty
#' #' @template roxlate-ml-weights-column
#' #' @template roxlate-ml-iter-max
#' #' @template roxlate-ml-options
#' #' @template roxlate-ml-dots
#' #'
#' #' @family Spark ML routines
#' #'
#' #' @export
#' ml_logistic_regression <- function(x,
#'                                    response,
#'                                    features,
#'                                    intercept = TRUE,
#'                                    alpha = 0,
#'                                    lambda = 0,
#'                                    weights.column = NULL,
#'                                    iter.max = 100L,
#'                                    ml.options = ml_options(),
#'                                    ...)
#' {
#'   ml_backwards_compatibility_api()
#'
#'   df <- spark_dataframe(x)
#'   sc <- spark_connection(df)
#'
#'   categorical.transformations <- new.env(parent = emptyenv())
#'   df <- ml_prepare_response_features_intercept(
#'     x = df,
#'     response = response,
#'     features = features,
#'     intercept = intercept,
#'     envir = environment(),
#'     categorical.transformations = categorical.transformations,
#'     ml.options = ml.options
#'   )
#'
#'   alpha <- ensure_scalar_double(alpha)
#'   lambda <- ensure_scalar_double(lambda)
#'   weights.column <- ensure_scalar_character(weights.column, allow.null = TRUE)
#'   iter.max <- ensure_scalar_integer(iter.max)
#'   only.model <- ensure_scalar_boolean(ml.options$only.model)
#'
#'   envir <- new.env(parent = emptyenv())
#'
#'   envir$id <- ml.options$id.column
#'   df <- df %>%
#'     sdf_with_unique_id(envir$id) %>%
#'     spark_dataframe()
#'
#'   tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)
#'
#'   envir$model <- "org.apache.spark.ml.classification.LogisticRegression"
#'   lr <- invoke_new(sc, envir$model)
#'
#'   model <- lr %>%
#'     invoke("setMaxIter", iter.max) %>%
#'     invoke("setFeaturesCol", envir$features) %>%
#'     invoke("setLabelCol", envir$response) %>%
#'     invoke("setFitIntercept", as.logical(intercept)) %>%
#'     invoke("setElasticNetParam", as.double(alpha)) %>%
#'     invoke("setRegParam", as.double(lambda))
#'
#'   if (!is.null(weights.column)) {
#'     model <- model %>%
#'       invoke("setWeightCol", weights.column)
#'   }
#'
#'   if (only.model)
#'     return(model)
#'
#'   fit <- model %>%
#'     invoke("fit", tdf)
#'
#'   # multinomial vs. binomial models have separate APIs for
#'   # retrieving results
#'   numClasses <- invoke(fit, "numClasses")
#'   isMultinomial <- numClasses > 2
#'
#'   # extract coefficients (can be either a vector or matrix, depending
#'   # on binomial vs. multinomial)
#'   coefficients <- if (isMultinomial) {
#'
#'     if (spark_version(sc) < "2.1.0") stop("Multinomial regression requires Spark 2.1.0 or higher.")
#'
#'     # multinomial
#'     coefficients <- read_spark_matrix(fit, "coefficientMatrix")
#'     colnames(coefficients) <- features
#'
#'     hasIntercept <- invoke(fit, "getFitIntercept")
#'     if (hasIntercept) {
#'       intercept <- read_spark_vector(fit, "interceptVector")
#'       coefficients <- cbind(intercept, coefficients)
#'       colnames(coefficients) <- c("(Intercept)", features)
#'     }
#'
#'     coefficients
#'
#'   } else {
#'
#'     coefficients <- read_spark_vector(fit, "coefficients")
#'
#'     hasIntercept <- invoke(fit, "getFitIntercept")
#'     if (hasIntercept) {
#'       intercept <- invoke(fit, "intercept")
#'       coefficients <- c(coefficients, intercept)
#'       names(coefficients) <- c(features, "(Intercept)")
#'     }
#'
#'     coefficients <- intercept_first(coefficients)
#'     coefficients
#'
#'   }
#'
#'   # multinomial models don't yet provide a 'summary' method
#'   # (as of Spark 2.1.0) so certain features will not be enabled
#'   # for those models
#'   areaUnderROC <- NA
#'   roc <- NA
#'   if (invoke(fit, "hasSummary")) {
#'     summary <- invoke(fit, "summary")
#'     areaUnderROC <- invoke(summary, "areaUnderROC")
#'     roc <- sdf_collect(invoke(summary, "roc"))
#'   }
#'
#'   model <- c(
#'     if (isMultinomial)
#'       "multinomial_logistic_regression"
#'     else
#'       "binomial_logistic_regression",
#'     "logistic_regression"
#'   )
#'
#'   ml_model(model, fit,
#'            features = features,
#'            response = response,
#'            intercept = intercept,
#'            weights.column = weights.column,
#'            coefficients = coefficients,
#'            roc = roc,
#'            area.under.roc = areaUnderROC,
#'            data = df,
#'            ml.options = ml.options,
#'            categorical.transformations = categorical.transformations,
#'            model.parameters = as.list(envir))
#' }
#'
#' #' @export
#' print.ml_model_logistic_regression <- function(x, ...) {
#'
#'   # report what model was fitted
#'   formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
#'   cat("Call: ", formula, "\n\n", sep = "")
#'
#'   # report coefficients
#'   cat("Coefficients:", sep = "\n")
#'   print(x$coefficients)
#' }
#'
#' #' @export
#' summary.ml_model_logistic_regression <- function(object, ...) {
#'   ml_model_print_call(object)
#'   print_newline()
#'   # ml_model_print_residuals(object)
#'   # print_newline()
#'   ml_model_print_coefficients(object)
#'   print_newline()
#' }
#'
