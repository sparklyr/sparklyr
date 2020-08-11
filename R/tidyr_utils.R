# This file contains helper methods that are useful for working with tidyr.

# emit an error if the given arg is missing
check_present <- function(x) {
  arg <- rlang::ensym(x)
  if (missing(x)) {
    abort(paste0("Argument `", arg, "` is missing with no default"))
  }
}

# helper method returning a minimal R dataframe containing the same set of
# column names as `sdf` does
columns <- function(sdf) {
  columns <- lapply(
    colnames(sdf),
    function(column) {
      v <- list(NA)
      names(v) <- column
      v
    }
  )
  do.call(data.frame, columns)
}

# helper method for updating dplyr group variables
update_group_vars <- function(input, output, preserved) {
  incl <- dplyr::group_vars(input)
  output <- do.call(dplyr::group_by, append(list(output), lapply(incl, as.symbol)))

  excl <- setdiff(incl, preserved)
  if (length(excl) > 0) {
    output <- do.call(dplyr::ungroup, append(list(output), lapply(excl, as.symbol)))
  }

  output
}

strip_names <- function(df, base, names_sep) {
  base <- paste0(base, names_sep)
  names <- names(df)

  has_prefix <- regexpr(base, names, fixed = TRUE) == 1L
  names[has_prefix] <- substr(names[has_prefix], nchar(base) + 1, nchar(names[has_prefix]))

  rlang::set_names(df, names)
}
