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

#' Pre-process the Inputs to a Spark ML Routine
#'
#' Pre-process / normalize the inputs typically passed to a
#' Spark ML routine.
#'
#' Pre-processing of these inputs typically involves:
#'
#' \enumerate{
#' \item Handling the case where \code{response} is itself a formula
#'       describing the model to be fit, thereby extracting the names
#'       of the \code{response} and \code{features} to be used,
#' \item Splitting categorical features into dummy variables (so they
#'       can easily be accommodated + specified in the underlying
#'       Spark ML model fit),
#' \item Mutating the associated variables \emph{in the specified environment}.
#' }
#'
#' Please take heed of the last point, as while this is useful in practice,
#' the behavior will be very surprising if you are not expecting it.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @param envir The \R environment in which the \code{response}, \code{features}
#'   and \code{intercept} bindings should be mutated. (Typically, the parent frame).
#' @param categorical.transformations An \R environment used to record what
#'   categorical variables were binarized in this procedure. Categorical
#'   variables that included in the model formula will be transformed into
#'   binary variables, and the generated mappings will be stored in this
#'   environment.
#' @template roxlate-ml-options
#'
#' @rdname ml_prepare_inputs
#' @rdname ml_prepare_inputs
#' @export
#'
#' @examples
#' \dontrun{
#' # note that ml_prepare_features, by default, mutates the 'features'
#' # binding in the same environment in which the function was called
#' local({
#'    ml_prepare_features(features = ~ x1 + x2 + x3)
#'    print(features) # c("x1", "x2", "x3")
#' })
#' }
ml_prepare_response_features_intercept <- function(x = NULL,
                                                   response,
                                                   features,
                                                   intercept,
                                                   envir = parent.frame(),
                                                   categorical.transformations = new.env(parent = emptyenv()),
                                                   ml.options = ml_options())
{
  # construct dummy data.frame from Spark DataFrame schema
  df <- x

  schema <- sdf_schema(df)
  names <- lapply(schema, `[[`, "name")
  rdf <- as.data.frame(names, stringsAsFactors = FALSE, optional = TRUE)

  # handle formulas as response
  if (is.formula(response)) {
    parsed <- parse_formula(response, data = rdf)
    response <- parsed$response
    features <- parsed$features
    intercept <- if (is.logical(parsed$intercept)) parsed$intercept
  }

  # for categorical features, split them into dummy variables
  # TODO: provide a mechanism for setting reference labels?
  features <- unlist(lapply(features, function(feature) {
    entry <- schema[[feature]]
    if (entry$type != "StringType")
      return(feature)

    # update data set with dummy variable columns
    auxiliary <- new.env(parent = emptyenv())
    df <<- ml_create_dummy_variables(df, feature, envir = auxiliary)

    # record transformations in env
    categorical.transformations[[feature]] <- auxiliary

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

  # apply na.action
  df <- apply_na_action(
    x         = df,
    response  = response,
    features  = features,
    na.action = ml.options$na.action
  )

  # return mutated dataset
  df
}

#' @rdname ml_prepare_inputs
#' @name   ml_prepare_inputs
#' @export
ml_prepare_features <- function(x,
                                features,
                                envir = parent.frame(),
                                ml.options = ml_options())
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

