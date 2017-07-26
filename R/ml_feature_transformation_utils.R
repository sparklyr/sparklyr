invoke_simple_transformer <- function(x, class, arguments, only.model = FALSE) {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  # generate transformer
  transformer <- invoke_new(sc, class)

  # apply arguments
  enumerate(arguments, function(key, val) {
    if (is.function(val))
      transformer <<- val(transformer, sdf)
    else if (!identical(val, NULL))
      transformer <<- invoke(transformer, key, val)
  })

  if (only.model) return(transformer)

  # invoke transformer
  transformed <- invoke(transformer, "transform", sdf)

  # register result
  sdf_register(transformed)
}
