validate_args_clustering <- function(.args) {
  .args[["k"]] <- forge::cast_integer(.args[["k"]])
  .args[["max_iter"]] <- forge::cast_scalar_integer(.args[["max_iter"]])
  .args[["seed"]] <- forge::cast_nullable_scalar_integer(.args[["seed"]])
  .args[["features_col"]] <- forge::cast_string(.args[["features_col"]])
  .args
}
