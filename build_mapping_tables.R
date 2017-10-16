#!/usr/bin/env Rscript
mapping_tables <- sparklyr:::ml_create_mapping_tables()

rlang::eval_tidy(rlang::expr(
  usethis::use_data(!!!rlang::syms(names(mapping_tables)), internal = TRUE, overwrite = TRUE)),
  data = mapping_tables
  )


