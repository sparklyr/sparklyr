invoke_simple_transformer <- function(x, class, arguments) {
  sdf <- spark_dataframe(x)
  sc <- spark_connection(sdf)

  # generate transformer
  transformer <- invoke_new(sc, class)

  # apply arguments
  enumerate(arguments, function(key, val) {
    if (is.function(val))
      transformer <<- val(transformer, sdf)
    else
      transformer <<- invoke(transformer, key, val)
  })

  # invoke transformer
  transformed <- invoke(transformer, "transform", sdf)

  # register result
  sdf_register(transformed)
}
