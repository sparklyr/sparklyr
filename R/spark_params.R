#' @importFrom rlang set_names
params_validate_and_set <- function(x, params = list()) {
  validated <- params_validate(x, params)
  new_names <- map_chr(names(validated), params_name_r_to_spark)
  names_set <- paste0("set", new_names)
  set_names(validated, names_set)
}

params_name_r_to_spark <- function(x) {
  sw <- strsplit(x, "_")[[1]]
  fl <- toupper(substr(sw, 1, 1))
  ll <- substr(sw, 2, nchar(sw))
  paste0(fl, ll, collapse = "")
}

#' @importFrom purrr imap
params_validate <- function(x, params) {
  pm <- params_validator(x)

  vals <- imap(
    params,
    ~ {
      fn <- pm[names(pm) == .y]
      if(length(fn) == 0) stop("Parameter ", .y, " not found")
      do.call(fn[[1]], list(x = .x))
    }
  )
  set_names(vals, names(params))
}

params_validator <- function(x) {
  UseMethod("params_validator")
}

params_validator.ml_estimator <- function(x) {
  params_base_validator(x)
}

params_base_validator <- function(x) {
  list(
    features_col = function(x) cast_string(x),
    label_col = function(x) cast_string(x),
    prediction_col = function(x) cast_string(x),
    elastic_net_param = function(x) cast_scalar_double(x),
    fit_intercept = function(x) cast_scalar_logical(x),
    reg_param = function(x) cast_scalar_double(x),
    max_iter = function(x) cast_scalar_integer(x),
    solver = function(x) cast_choice(x, c("auto", "l-bfgs", "normal")),
    standardization = function(x) cast_scalar_logical(x),
    tol = function(x) cast_scalar_double(x),
    loss = function(x) cast_choice(x, c("squaredError", "huber")),
    weight_col = function(x) cast_nullable_string(x)
  )
}
