new_ml_evaluator <- function(jobj, ..., class = character()) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(class, "ml_evaluator")
  )
}

#' @export
spark_jobj.ml_evaluator <- function(x, ...) {
  x$.jobj
}

#' @export
print.ml_evaluator <- function(x, ...) {
  cat(ml_short_type(x), "(Evaluator) \n")
  cat(paste0("<", x$uid, ">"), "\n")
  ml_print_column_name_params(x)
  cat(" (Evaluation Metric)\n")
  cat(paste0("  ", "metric_name: ", ml_param(x, "metric_name")))
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_evaluator <- function(x, dataset) {
  x %>%
    spark_jobj() %>%
    invoke("evaluate", spark_dataframe(dataset))
}
