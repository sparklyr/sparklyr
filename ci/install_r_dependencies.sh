#!/bin/bash

export MAKEFLAGS="-j$(($(nproc) + 1))"

R_REMOTES_NO_ERRORS_FROM_WARNINGS=true Rscript <(
cat << _RSCRIPT_EOF_
  install.packages("remotes")
  library(remotes)
  for (x in c("r-lib/httr", "omegahat/RCurl"))
    try(remotes::install_github(x))
  remotes::install_deps(dependencies = c("Imports", "Suggests"))
_RSCRIPT_EOF_
)
