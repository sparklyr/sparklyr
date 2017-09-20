new_ml_summary <- function(jobj, ..., subclass = NULL) {
  structure(
    list(
      uid = jobj$uid,
      type = jobj_info(jobj)$class,
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_summary")
  )
}
