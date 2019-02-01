ml_get_constructor <- function(jobj) {
  jobj %>%
    jobj_class(simple_name = FALSE) %>%
    purrr::map(ml_map_class) %>%
    purrr::compact() %>%
    purrr::map(~ paste0("new_", .x)) %>%
    purrr::keep(~ exists(.x, where = asNamespace("sparklyr"), mode = "function")) %>%
    rlang::flatten_chr() %>%
    head(1)
}

#' Wrap a Spark ML JVM object
#'
#' Identifies the associated sparklyr ML constructor for the JVM object by inspecting its
#'   class and performing a lookup. The lookup table is specified by the
#'   `sparkml/class_mapping.json` files of sparklyr and the loaded extensions.
#'
#' @param jobj The jobj for the pipeline stage.
#'
#' @keywords internal
#' @export
ml_call_constructor <- function(jobj) {
  do.call(ml_get_constructor(jobj), list(jobj = jobj))
}

new_ml_pipeline_stage <- function(jobj, ..., class = character()) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(class, "ml_pipeline_stage")
  )
}
