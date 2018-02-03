ml_options <- function(id.column       = random_string("id"),
                       response.column = random_string("response"),
                       features.column = random_string("features"),
                       output.column   = random_string("output"),
                       model.transform = NULL,
                       only.model      = FALSE,
                       na.action       = getOption("na.action", "na.omit"),
                       ...)
{
  options <- list(
    id.column       = ensure_scalar_character(id.column),
    response.column = ensure_scalar_character(response.column),
    features.column = ensure_scalar_character(features.column),
    output.column   = ensure_scalar_character(output.column),
    model.transform = model.transform,
    only.model      = ensure_scalar_boolean(only.model),
    na.action       = na.action,
    ...
  )

  class(options) <- "ml_options"
  options
}
