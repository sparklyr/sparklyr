is.formula <- function(x) {
  inherits(x, "formula")
}

parse_formula <- function(formula, data = NULL) {
  formula <- validate_formula(formula)
  n <- length(formula)

  # extract response
  response <- if (n == 3) {
    lhs <- formula[[2]]
    if (!(is.symbol(lhs) || is.character(lhs)))
      stop("expected symbolic response; got '", lhs, "'")
    as.character(lhs)
  }

  # extract features
  terms <- stats::terms(formula, data = data)
  features <- attr(terms, "term.labels")
  intercept <- as.logical(attr(terms, "intercept"))

  list(features = features,
       response = response,
       intercept = intercept)
}

ml_prepare_features <- function(x,
                                features,
                                envir = parent.frame(),
                                ml.options = ml.options())
{
  if (is.formula(features)) {
    parsed <- parse_formula(features)
    features <- parsed$features
  }

  features <- as.character(features)

  assign("features", features, envir = envir)

  if (missing(x))
    return(features)

  # apply na.action
  df <- apply_na_action(
    x = x,
    response = NULL,
    features = features,
    na.action = ml.options$na.action
  )

  # return mutated dataset
  df
}
