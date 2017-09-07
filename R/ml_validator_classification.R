#' @export
ml_validator_logistic_regression <- function(.args, .current_args) {
  input_arg_names <- names(.args)
  current_arg_names <- names(.current_args) %>%
    setdiff(input_arg_names)
  .args <- c(.current_args[current_arg_names], .args)
  .args$elastic_net_param <- ensure_scalar_double(.args$elastic_net_param)
  .args$reg_param <- ensure_scalar_double(.args$reg_param)
  .args$max_iter <- ensure_scalar_integer(.args$max_iter)
  .args[input_arg_names]
}
