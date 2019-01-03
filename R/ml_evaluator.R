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
  cat(paste0("<", x$uid, ">"),"\n")
  ml_print_column_name_params(x)
  cat(" (Evaluation Metric)\n")
  cat(paste0("  ", "metric_name: ", ml_param(x, "metric_name")))
}

#' Spark ML -- Evaluate prediction frames with evaluators
#'
#' Evaluate a prediction dataset with a Spark ML evaluator
#'
#' @param x A \code{ml_evaluator} object.
#' @param dataset A \code{spark_tbl} with columns as specified in the evaluator object.
#' @export
ml_evaluate <- function(x, dataset) {
  x %>%
    spark_jobj() %>%
    invoke("evaluate", spark_dataframe(dataset))
}
