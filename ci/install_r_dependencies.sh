#!/bin/bash

export MAKEFLAGS="-j$(($(nproc) + 1))"

R_REMOTES_NO_ERRORS_FROM_WARNINGS=true Rscript <(
cat << _RSCRIPT_EOF_
  install.packages("devtools")
  devtools::install_deps()
_RSCRIPT_EOF_
)
