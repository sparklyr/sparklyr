ml_backwards_compatibility_api <- function(envir = parent.frame()) {
  # retrieve dot arguments from envir
  dots <- eval(quote(list(...)), envir = envir)

  # allow 'max.iter' as a backwards compatible alias for 'iter.max'
  if (is.null(envir$iter.max) && !is.null(dots$max.iter))
    assign("iter.max", dots$max.iter, envir = envir)

  # allow 'only_model' as alias for 'only.model'
  if (is.logical(dots$only_model) && !is.null(envir$ml.options)) {
    ml.options <- envir$ml.options
    ml.options$only.model <- dots$only_model
    assign("ml.options", ml.options, envir = envir)
  }

  # allow 'input_col' as alias for input.col
  if (is.null(envir$input.col) && !is.null(dots$input_col))
    assign("input.col", dots$input_col, envir = envir)

  # allow 'output_col' as alias for output.col
  if (is.null(envir$output.col) && !is.null(dots$output_col))
    assign("output.col", dots$output_col, envir = envir)

  # allow 'data' as an optional parameter
  if (!is.null(dots$data) && is.formula(envir$x)) {
    assign("response", envir$x, envir = envir)
    assign("x", dots$data, envir = envir)
  }
}
