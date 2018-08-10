validate_args_clustering <- function(.args) {
  .args[["k"]] <- cast_integer(.args[["k"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args[["features_col"]] <- cast_string(.args[["features_col"]])
  .args
}
