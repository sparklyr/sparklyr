ml_backwards_compatibility_api <- function(envir = parent.frame()) {
  # retrieve dot arguments from envir
  dots <- eval(parse(text = "list(...)"), envir = envir)

  # allow 'max.iter' as a backwards compatible alias for 'iter.max'
  if (is.null(envir$iter.max) && !is.null(dots$max.iter))
    assign("iter.max", dots$max.iter, envir = envir)
}
