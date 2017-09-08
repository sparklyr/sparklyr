ml_validator_logistic_regression <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    elastic_net_param <- ensure_scalar_double(elastic_net_param)
    reg_param <- ensure_scalar_double(reg_param)
    max_iter <- ensure_scalar_integer(max_iter)
  }) %>%
    `[`(names(args))
}
