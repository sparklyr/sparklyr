ml_index_labels_metadata <- function(pipeline_model, dataset, label_col) {
  label_indexer_model <- ml_stages(pipeline_model) %>%
    dplyr::nth(-2) # second from last, either RFormulaModel or StringIndexerModel
  transformed_tbl <- ml_transform(label_indexer_model, dataset)
  label_col <- if (inherits(label_indexer_model, "ml_r_formula_model"))
    ml_param(label_indexer_model, "label_col")
  else
    ml_param(label_indexer_model, "output_col")

  ml_column_metadata(transformed_tbl, label_col) %>%
    `[[`("vals")
}

ml_feature_names_metadata <- function(pipeline_model, dataset, features_col) {
  r_formula_model <- ml_stage(pipeline_model, 1)
  transformed_tbl <- ml_transform(r_formula_model, dataset)
  features_col <- ml_param(r_formula_model, "features_col")

  ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(!!rlang::sym("idx")) %>%
    dplyr::pull("name")
}

ml_generate_ml_model <- function(x, predictor, formula, features_col = "features",
                                 label_col = "label", type,
                                 constructor) {
  sc <- spark_connection(x)
  classification <- identical(type, "classification")

  r_formula <- if (classification) {
    if (spark_version(sc) >= "2.1.0")
      ft_r_formula(sc, formula, features_col, label_col,
                   force_index_label = TRUE)
    else
      ft_r_formula(sc, formula, features_col, random_string(label_col))
  } else
    ft_r_formula(sc, formula, features_col, label_col)

  string_indexer <- if (classification && spark_version(sc) < "2.1.0") {
    response_col <- formula %>%
      strsplit("~", fixed = TRUE) %>%
      rlang::flatten_chr() %>%
      head(1) %>%
      trimws()

    ft_string_indexer(sc, response_col, label_col)
  }

  pipeline <- do.call(ml_pipeline,
                      Filter(length, list(r_formula, string_indexer, predictor))
  )

  pipeline_model <- pipeline %>%
    ml_fit(x)

  feature_names <- ml_feature_names_metadata(pipeline_model, x, features_col)

  call <- sys.call(sys.parent())
  call_string <- paste(deparse(call, width.cutoff = 500), " ")

  args <- list(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = ml_stages(pipeline_model) %>% dplyr::last(),
    dataset = x,
    formula = formula,
    feature_names = feature_names,
    call = call_string
  ) %>%
    (function(args) if (classification) rlang::modify(
      args, index_labels = ml_index_labels_metadata(pipeline_model, x, label_col)
    ) else args)

  do.call(constructor, args)
}
