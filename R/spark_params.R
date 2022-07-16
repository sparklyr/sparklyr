params_validator <- function(x) {
  UseMethod("params_validator")
}

params_validator.ml_estimator <- function(x) {
  list(
    features_col = function(x) x,
    # label_col = x,
    # prediction_col = x,
    elastic_net_param = function(x) cast_scalar_double(x),
    # fit_intercept = cast_scalar_logical(x),
    # reg_param = cast_scalar_double(x),
    # max_iter = cast_scalar_integer(x),
    solver = function(x) cast_choice(x, c("auto", "l-bfgs", "normal"))
    # standardization = cast_scalar_logical(x),
    # tol = cast_scalar_double(x),
    # loss = x,
    # weight_col = cast_nullable_string(x)
  )
}

#' @importFrom purrr imap
params_validate <- function(x, ...) {
  vars <- enexprs(...)
  pm <- params_validator(x)

  vals <- purrr::imap(
    vars,
    ~ {
      fn <- pm[names(pm) == .y]
      if(length(fn) == 0) stop("Parameter ", .y, " not found")
      do.call(fn[[1]], list(x = .x))
    }
  )

  set_names(vals, names(vars))
}

params_to_spark <- function(x, ...) {
  validated <- params_validate(x, ... = ...)
  new_names <- map_chr(names(validated), params_name_r_to_spark)
  set_names(validated, new_names)
}

params_name_r_to_spark <- function(x) {
  sw <- strsplit(x, "_")[[1]]
  fl <- toupper(substr(sw, 1, 1))
  ll <- substr(sw, 2, nchar(sw))
  paste0(fl, ll, collapse = "")
}
