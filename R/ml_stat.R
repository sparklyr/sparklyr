#' Compute correlation matrix
#'
#' @param x A \code{tbl_spark}.
#' @param columns The names of the columns to calculate correlations of. If only one
#'   column is specified, it must be a vector column (for example, assembled using
#'   \code{ft_vector_assember()}).
#' @param method The method to use, either \code{"pearson"} or \code{"spearman"}.
#'
#' @return A correlation matrix organized as a data frame.
#' @export
ml_corr <- function(x, columns = NULL, method = c("pearson", "spearman")) {
  if (spark_version(spark_connection(x)) < "2.2.0")
    stop("`ml_corr()` requires Spark 2.2.0+")
  method <- match.arg(method)

  columns <- if (rlang::is_null(columns)) colnames(x) else {
    sapply(columns, ensure_scalar_character)
  }

  col_in_df <- columns %in% colnames(x)
  if (!all(col_in_df)) {
    bad_cols <- paste0(columns[!col_in_df], collapse = ", ")
    stop("All columns specified must be in x. Failed to find ",
         bad_cols, ".", call. = FALSE)
  }

  if (identical(length(columns), 1L)) {
    # check to see that the column is a VectorUDT

    if (!grepl("VectorUDT", sdf_schema(x)[[columns]][["type"]]))
      stop("When only one column is specified, it must be a column of Vectors. For example, the output of `ft_vector_assembler()`.",
           call. = FALSE)

    num_features <- x %>%
      dplyr::select(!!columns) %>%
      spark_dataframe() %>%
      invoke("first") %>%
      sapply(invoke, "size")
    feature_names <- num_features %>%
      paste0("V", seq_len(.))

    features_col <- columns
    sdf <- spark_dataframe(x)
  } else {
    num_features <- length(columns)
    feature_names <- columns
    features_col <- random_string("features")
    sdf <- x %>%
      ft_vector_assembler(columns, features_col) %>%
      spark_dataframe()
  }

  invoke_static(spark_connection(sdf),
                "org.apache.spark.ml.stat.Correlation",
                "corr", sdf, features_col) %>%
    invoke("first") %>%
    sapply(invoke, "toArray") %>%
    matrix(nrow = num_features) %>%
    as.data.frame() %>%
    dplyr::rename(!!!rlang::set_names(paste0("V", seq_len(num_features)),
                                      feature_names)
                  )
}

#' Chi-square hypothesis testing for categorical data.
#'
#' Conduct Pearson's independence test for every feature against the
#'   label. For each feature, the (feature, label) pairs are converted
#'   into a contingency matrix for which the Chi-squared statistic is
#'   computed. All label and feature values must be categorical.
#'
#' @param x A \code{tbl_spark}.
#' @param features The name(s) of the feature columns. This can also be the name
#'   of a single vector column created using \code{ft_vector_assembler()}.
#' @param label The name of the label column.
#' @return A data frame with one row for each (feature, label) pair with p-values,
#'   degrees of freedom, and test statistics.
#' @export
ml_chisquare_test <- function(x, features, label) {
  if (spark_version(spark_connection(x)) < "2.2.0")
    stop("`ml_chisquare_test()` requires Spark 2.2.0+")
  schema <- sdf_schema(x)

  columns <- c(features, label)
  col_in_df <- columns %in% colnames(x)
  if (!all(col_in_df)) {
    bad_cols <- paste(columns[!col_in_df], collapse = ", ")
    stop("All columns specified must be in x. Failed to find ",
         bad_cols, ".", call. = FALSE)
  }

  label_is_double <- identical(schema[[label]][["type"]], "DoubleType")
  features_are_assembled <- identical(length(features), 1L) &&
    grepl("VectorUDT", schema[[features]][["type"]])

  if (features_are_assembled) {
    features_col <- features
    feature_names <- x %>%
      dplyr::select(!!features) %>%
      spark_dataframe() %>%
      invoke("first") %>%
      sapply(invoke, "size") %>%
      paste0("feature_", seq_len(.))
    if (label_is_double) {
      label_col <- label
      df <- x
    } else {
      label_col <- random_string("label")
      df <- ft_string_indexer(x, label, label_col)
    }
  } else {
    feature_names <- features
    features_col <- random_string("features")
    if (label_is_double) {
      label_col <- label
      df <- ft_r_formula(x, paste0("~ ", paste0(features, collapse = " + ")),
                         features_col = features_col)
    } else {
      label_col <- random_string("label")
      df <- ft_r_formula(x, paste0(label, " ~ ",
                                   paste0(features, collapse = " + ")),
                         features_col = features_col,
                         label_col = label_col, force_index_label = TRUE
      )
    }
  }

  sdf <- spark_dataframe(df)
  sc <- spark_connection(sdf)

  invoke_static(sc, "org.apache.spark.ml.stat.ChiSquareTest", "test",
                sdf, features_col, label_col) %>%
    sdf_register() %>%
    dplyr::collect() %>%
    tidyr::unnest() %>%
    dplyr::transmute(
      p_value = !!rlang::sym("pValues"),
      degrees_of_freedom = unlist(!!rlang::sym("degreesOfFreedom")),
      statistic = !!rlang::sym("statistics")
    ) %>%
    dplyr::bind_cols(data.frame(feature = feature_names,
                                label = label), .)
}
