ml_backwards_compatibility_api <- function(envir = parent.frame()) {
  # retrieve dot arguments from envir
  dots <- eval(parse(text = "list(...)"), envir = envir)

  # allow 'max.iter' as a backwards compatible alias for 'iter.max'
  if (is.null(envir$iter.max) && !is.null(dots$max.iter))
    assign("iter.max", dots$max.iter, envir = envir)

  # allow 'only_model' as alias for 'only.model'
  if (is.logical(dots$only_model) && !is.null(envir$ml.options)) {
    ml.options <- envir$ml.options
    ml.options$only.model <- dots$only_model
    assign("ml.options", ml.options, envir = envir)
  }
}
