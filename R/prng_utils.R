gen_prng_seed <- function() {
  if (is.null(get0(".Random.seed"))) {
    NULL
  } else {
    as.integer(sample.int(.Machine$integer.max, size = 1L))
  }
}
