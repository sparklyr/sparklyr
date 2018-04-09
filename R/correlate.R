#' @importFrom corrr correlate
#' @export
correlate.tbl_spark <- function(x, y = NULL,
                              use = "pairwise.complete.obs",
                              method = "pearson",
                              diagonal = NA,
                              quiet = FALSE) {

  if(method != "pearson")   stop("Only person method is currently supported")
  if(use != "complete.obs") stop("Only 'complete.obs' method are supported")
  if(!is.null(y))           stop("y is not supported for tables with a SQL back-end")
  if(!is.na(diagonal))      stop("Only NA's are supported for same field correlations")

  df <- x
  col_names <- colnames(df)
  col_no <- length(col_names)

  combos <- 1:(col_no - 1) %>%
    purrr:: map_df(~dplyr::tibble(
      x = col_names[.x],
      y = col_names[(.x + 1):col_no],
      cn = paste0(x, "_", y)
    ))

  full_combos <- 1:(col_no) %>%
    purrr::map_df(~
             dplyr::tibble(
               x = col_names[.x],
               y = col_names[.x:col_no],
               cn = paste0(x, "_", y)
             ) %>%
             bind_rows(
               dplyr::tibble(
                 x = col_names[.x:col_no],
                 y = col_names[.x],
                 cn = paste0(y, "_", x)
               )
             )) %>%
    filter(x != y)

  df <- df %>%
    filter_all(all_vars(!is.na(.)))

  cor_f <- 1:nrow(combos) %>%
    purrr::map(
      ~ rlang::expr(cor(
        !! rlang::sym(combos$x[.x]),
        !! rlang::sym(combos$y[.x])))
    )

  cor_f <- purrr::set_names(cor_f, combos$cn)

  df_cor <- df %>%
    summarise(!!! cor_f) %>%
    collect() %>%
    dplyr::as_tibble() %>%
    tidyr::gather(cn, cor) %>%
    right_join(full_combos, by = "cn") %>%
    select(-cn) %>%
    tidyr::spread(y, cor) %>%
    rename(rowname = x)
    # Put cols and rows into original order
    # select(rowname, !!col_names) %>%
    # {.[match(col_names, .$rowname),]}

  class(df_cor) <- c("cor_df", class(df_cor))

  if (!quiet)
    message("\nCorrelation method: '", method, "'",
            "\nMissing treated using: '", use, "'\n")

  df_cor
}
