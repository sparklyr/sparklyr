ml_index_labels_metadata <- function(pipeline_model, dataset, label_col) {
  r_formula_model <- ml_stage(pipeline_model, 1)
  transformed_tbl <- ml_transform(r_formula_model, dataset)
  label_col <- ml_param(r_formula_model, "label_col")

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
    arrange(!!rlang::sym("idx")) %>%
    dplyr::pull("name")
}

ml_generate_ml_model <- function(x, predictor, formula, features_col, label_col, type,
                                 constructor) {
  sc <- spark_connection(x)
  classification <- identical(type, "classification")
  r_formula <- ft_r_formula(sc, formula, features_col, label_col,
                            force_index_label = if (classification) TRUE else FALSE,
                            dataset = x)
  pipeline <- ml_pipeline(r_formula, predictor)
  pipeline_model <- pipeline %>%
    ml_fit(x)

  feature_names <- ml_feature_names_metadata(pipeline_model, x, features_col)

  call <- sys.call(sys.parent())
  call_string <- paste(deparse(call, width.cutoff = 500), " ")

  args <- list(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = ml_stage(pipeline_model, 2),
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
