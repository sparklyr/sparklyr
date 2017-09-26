ml_validator_logistic_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  ml_apply_validation(
    {
      bind_old_to_new(old_new_mapping)
      elastic_net_param <- ensure_scalar_double(elastic_net_param)
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      family <- rlang::arg_match(family, c("auto", "binomial", "multinomial"))
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      threshold <- ensure_scalar_double(threshold)
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
    },
    args, nms, old_new_mapping
  )
}

