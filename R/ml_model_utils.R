ml_index_labels_metadata <- function(label_indexer_model, dataset, label_col) {
  transformed_tbl <- ml_transform(label_indexer_model, dataset)
  label_col <- if (inherits(label_indexer_model, "ml_r_formula_model")) {
    ml_param(label_indexer_model, "label_col")
  } else {
    ml_param(label_indexer_model, "output_col")
  }

  ml_column_metadata(transformed_tbl, label_col) %>%
    `[[`("vals")
}

ml_feature_names_metadata <- function(pipeline_model, dataset, features_col) {
  preprocessor <- ml_stage(pipeline_model, 1)
  transformed_tbl <- ml_transform(preprocessor, dataset)
  features_col <- if (inherits(preprocessor, "ml_r_formula_model")) {
    ml_param(preprocessor, "features_col")
  } else { # vector assembler
    ml_param(preprocessor, "output_col")
  }

  ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(!!rlang::sym("idx")) %>%
    dplyr::pull("name")
}
