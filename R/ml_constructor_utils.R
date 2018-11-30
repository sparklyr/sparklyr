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

ml_constructor_dispatch <- function(jobj) {
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
