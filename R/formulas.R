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

validate_formula <- function(formula) {
  formula <- as.formula(formula)
  for (i in 2:length(formula))
    validate_formula_operators(formula[[i]])
  formula
}

validate_formula_operators <- function(object) {
  n <- length(object)
  if (is.call(object) && n > 0) {

    # check that this is a call to a known operator
    op <- object[[1]]
    if (!is.symbol(op))
      stop("expected a symbol for call; got '", deparse(op), "'")

    ch <- as.character(op)
    if (!ch %in% c("+", "-", "("))
      stop("unhandled formula operator: expected '+' or '-'; got '", ch, "'")

    # validate the rest of the calls
    for (i in 1:n)
      validate_formula_operators(object[[i]])
  }
}

prepare_features <- function(df, features, envir = parent.frame()) {

  if (is.formula(features)) {
    parsed <- parse_formula(features)
    features <- parsed$features
  }

  features <- as.character(features)

  assign("features", features, envir = envir)
}

prepare_response_features_intercept <- function(df,
                                                response,
                                                features,
                                                intercept,
                                                envir = parent.frame())
{
  # construct dummy data.frame from Spark DataFrame schema
  schema <- sdf_schema(df)
  names <- lapply(schema, `[[`, "name")
  rdf <- as.data.frame(names, stringsAsFactors = FALSE)

  # handle formulas as response
  if (is.formula(response)) {
    parsed <- parse_formula(response, data = rdf)
    response <- parsed$response
    features <- parsed$features
    intercept <- if (is.logical(parsed$intercept)) parsed$intercept
  }

  # for categorical features, split them into dummy variables
  # TODO: provide a mechanism for setting reference labels?
  auxiliary <- new.env(parent = emptyenv())
  features <- unlist(lapply(features, function(feature) {
    entry <- schema[[feature]]
    if (entry$type != "StringType")
      return(feature)

    # update data set with dummy variable columns
    df <<- sdf_create_dummy_variables(df, feature, envir = auxiliary)

    # drop one level (to avoid perfect multi-collinearity)
    tail(auxiliary$columns, n = -1)
  }))

  # ensure output format
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  intercept <- if (!is.null(intercept)) ensure_scalar_boolean(intercept)

  # mutate in environment
  assign("response", response, envir = envir)
  assign("features", features, envir = envir)
  assign("intercept", intercept, envir = envir)

  # return mutated dataset
  df
}
