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
    offset_col = function(x) cast_nullable_string(x),
    # AFT
    censor_col = function(x) cast_string(x),
    quantile_probabilities = function(x) cast_double_list(x),
    aggregation_depth = function(x) cast_scalar_integer(x),
    quantiles_col = function(x) cast_nullable_string(x),
    # Decision tree
    impurity = function(x) cast_choice(x, c("variance")),
    checkpoint_interval = function(x) cast_scalar_integer(x),
    max_bins = function(x) cast_scalar_integer(x),
    max_depth = function(x) cast_scalar_integer(x),
    min_info_gain = function(x) cast_scalar_double(x),
    min_instances_per_node = function(x) cast_scalar_integer(x),
    cache_node_ids = function(x) cast_scalar_logical(x),
    max_memory_in_mb = function(x) cast_scalar_integer(x),
    variance_col = function(x) cast_nullable_string(x),
    seed = function(x) cast_nullable_scalar_integer(x),
    # Isotonic
    feature_index = function(x) cast_scalar_integer(x),
    isotonic = function(x) cast_scalar_logical(x),
    # Random Forest
    num_trees = function(x) cast_scalar_integer(x),
    subsampling_rate = function(x) cast_scalar_double(x),
    feature_subset_strategy = function(x) cast_string(x),
    # GBT
    step_size = function(x) cast_scalar_double(x),
    loss_type = function(x) cast_choice(x, c("squared", "absolute"))
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
  new_names <- map_chr(
    names(validated),
    param_name_r_to_spark,
    as.list(genv_get_param_mapping_r_to_s())
    )
  names_set <- paste0("set", new_names)
  set_names(validated, names_set)
}

param_name_r_to_spark <- function(x, initial_catalog = NULL) {
  match_catalog <- initial_catalog[names(initial_catalog) == x]
  if(length(match_catalog) == 0) {
    sw <- strsplit(x, "_")[[1]]
  } else {
    sw <- match_catalog[[1]]
  }
  fl <- toupper(substr(sw, 1, 1))
  ll <- substr(sw, 2, nchar(sw))
  trimws(paste0(fl, ll, collapse = ""))
}

#' @importFrom purrr imap
params_validate <- function(x, params, unmatched_fail = FALSE) {
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
      if(!is.null(params[[i]])) {
        new_val <- do.call(fn[[1]], list(x = params[[i]]))
        if(is.null(new_val) != is.null(params[[i]])) stop("NULL value not valid")
        if(!is.null(new_val)) params[[i]] <- new_val
      }
    }
  }
  params
}
