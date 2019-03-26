find_in_extensions <- function(what) {

  # Get package namespaces for sparkly and extensions.
  namespaces <- c("sparklyr", .globals$extension_packages) %>%
    purrr::map(asNamespace)

  (function(what, namespaces) {
    if (!length(namespaces)) return(NULL)

    # Look in `namespaces` one at a time for the function
    purrr::possibly(get, NULL)(what, envir = namespaces[[1]], mode = "function") %||%
      Recall(what, namespaces[-1])
  })(what, namespaces)
}

find_constructor <- function(candidates, jobj) {
  if (!length(candidates)) stop("Constructor not found for `", jobj_class(jobj)[[1]], "`.", call. = FALSE)

  # For each candidate function, look in extension namespaces, and return the first one found
  find_in_extensions(candidates[[1]]) %||% Recall(candidates[-1])
}

ml_get_constructor <- function(jobj) {
  jobj %>%
    jobj_class(simple_name = FALSE) %>%
    purrr::map(ml_map_class) %>%
    purrr::compact() %>%
    purrr::map(~ paste0("new_", .x)) %>%
    find_constructor(jobj)
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
