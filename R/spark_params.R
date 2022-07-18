params_validator <- function(x) {
  UseMethod("params_validator")
}

params_validator.ml_estimator <- function(x) {
  params_base_validator(x)
}

params_validator.pre_ml_estimator <- function(x) {
  params_base_validator(x)
}

params_base_validator <- function(x) {
  list(
    #LR
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
    weight_col = function(x) cast_nullable_string(x),
    # GLM
    family = function(x) cast_choice(x, c("gaussian", "binomial", "poisson", "gamma", "tweedie")),
    link = function(x) cast_nullable_string(x),
    link_power = function(x) cast_nullable_scalar_double(x),
    variance_power = function(x) cast_nullable_scalar_double(x),
    link_prediction_col = function(x) cast_nullable_string(x),
    offset_col = function(x) cast_nullable_string(x)
  )
}

params_validate_estimator <- function(jobj, params = list()) {
  dummy_obj <- list()

  spark_obj_name <- jobj_info(jobj)[[1]]
  class_mapping <- as.list(genv_get_ml_class_mapping())
  r_obj_class <- class_mapping[names(class_mapping) == spark_obj_name][[1]]

  class(dummy_obj) <- c(r_obj_class, "pre_ml_estimator")
  params_validate(dummy_obj, params = params)
}

#' @importFrom rlang set_names
params_validate_estimator_and_set <- function(jobj, params = list()) {
  validated <- params_validate_estimator(jobj, params)
  new_names <- map_chr(names(validated), params_name_r_to_spark)
  names_set <- paste0("set", new_names)
  set_names(validated, names_set)
}


params_name_r_to_spark <- function(x) {
  sw <- strsplit(x, "_")[[1]]
  fl <- toupper(substr(sw, 1, 1))
  ll <- substr(sw, 2, nchar(sw))
  trimws(paste0(fl, ll, collapse = ""))
}

#' @importFrom purrr imap
params_validate <- function(x, params, unmatched_fail = TRUE) {
  pm <- params_validator(x)

  matched_list <- imap(params, ~ any(names(pm) == .y))
  matched_names <- as.logical(matched_list)

  if(unmatched_fail) {
    if(!all(matched_names)) {
      unmatched_list <- names(params)[!matched_names]
      stop(
        "Could not find the following parameter(s): ",
        paste0(unmatched_list, collapse = ", ")
      )
    }
  }

  names_params <- names(params)
  for(i in seq_along(params)) {
    if(matched_names[i]) {
      fn <- pm[names(pm) == names_params[i]]
      new_val <- do.call(fn[[1]], list(x = params[[i]]))
      if(is.null(new_val) != is.null(params[[i]])) stop("NULL value not valid")
      if(!is.null(new_val)) params[[i]] <- new_val
    }
  }
  params
}
