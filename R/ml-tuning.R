#' @export
ml_param_grid <- function(param_list) {
  names(param_list) %>% # stages
    lapply(function(stage) {
      stage_params <- param_list[[stage]]
      params <- names(stage_params)
      setNames(lapply(params, function(param) {
        param_values <- stage_params[[param]]
        lapply(param_values, function(value) {
          list(stage = stage, param = param, value = value)
        })
      }), paste(stage, params, sep = "-"))
    }) %>%
    rlang::flatten() %>%
    expand.grid(stringsAsFactors = FALSE) %>%
    apply(1, list) %>%
    rlang::flatten()
}
