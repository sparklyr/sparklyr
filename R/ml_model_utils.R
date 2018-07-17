ml_index_labels_metadata <- function(label_indexer_model, dataset, label_col) {

  transformed_tbl <- ml_transform(label_indexer_model, dataset)
  label_col <- if (inherits(label_indexer_model, "ml_r_formula_model"))
    ml_param(label_indexer_model, "label_col")
  else
    ml_param(label_indexer_model, "output_col")

  ml_column_metadata(transformed_tbl, label_col) %>%
    `[[`("vals")
}

ml_feature_names_metadata <- function(pipeline_model, dataset, features_col) {
  preprocessor <- ml_stage(pipeline_model, 1)
  transformed_tbl <- ml_transform(preprocessor, dataset)
  features_col <- if (inherits(preprocessor, "ml_r_formula_model"))
    ml_param(preprocessor, "features_col")
  else # vector assembler
    ml_param(preprocessor, "output_col")

  ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(!!rlang::sym("idx")) %>%
    dplyr::pull("name")
}

ml_generate_ml_model <- function(
  x, predictor, formula, features_col = "features",
  label_col = "label", type,
  constructor, predicted_label_col = NULL) {
  sc <- spark_connection(x)
  classification <- identical(type, "classification")

  pipeline <- if (classification) {
    r_formula <- ft_r_formula(sc, formula, features_col, label_col,
                              force_index_label = FALSE)
    ml_pipeline(r_formula, predictor)
  } else if (identical(type, "clustering") && spark_version(sc) < "2.0.0") {
    # one-sided formulas not supported prior to Spark 2.0
    rdf <- sdf_schema(x) %>%
      lapply(`[[`, "name") %>%
      as.data.frame(stringsAsFactors = FALSE)
    features <- stats::terms(as.formula(formula), data = rdf) %>%
      attr("term.labels")

    vector_assembler <- ft_vector_assembler(
      sc, input_cols = features, output_col = features_col
    )
    ml_pipeline(vector_assembler, predictor)

  } else {
    r_formula <- ft_r_formula(sc, formula, features_col, label_col)
    ml_pipeline(r_formula, predictor)
  }

  pipeline_model <- pipeline %>%
    ml_fit(x)

  # for pipeline, fix data prep transformation but use the un-fitted estimator predictor
  pipeline <- pipeline_model %>%
    ml_stages() %>%
    head(-1) %>%
    rlang::invoke(ml_pipeline, ., uid = ml_uid(pipeline_model)) %>%
    ml_add_stage(predictor)

  if (classification) {
    label_indexer_model <- ml_stages(pipeline_model) %>%
      dplyr::nth(-2) # second from last, either RFormulaModel or StringIndexerModel
    index_labels <- ml_index_labels_metadata(label_indexer_model, x, label_col)
    index_to_string <- ft_index_to_string(
      sc, ml_param(predictor, "prediction_col"), predicted_label_col, index_labels)
    pipeline <- pipeline %>%
      ml_add_stage(index_to_string)
    pipeline_model <- pipeline_model %>%
      ml_add_stage(index_to_string) %>%
      # ml_fit() here doesn't do any actual computation but simply
      #   returns a PipelineModel since ml_add_stage() returns a
      #   Pipeline (Estimator)
      ml_fit(x)
  }

  # workaround for https://issues.apache.org/jira/browse/SPARK-19953
  model_uid <- if (spark_version(sc) < "2.2.0")
    switch(class(predictor)[[1]],
           ml_random_forest_regressor = "rfr",
           ml_random_forest_classifier = "rfc",
           ml_uid(predictor))
  else
    ml_uid(predictor)

  feature_names <- ml_feature_names_metadata(pipeline_model, x, features_col)

  args <- list(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = ml_stage(pipeline_model, model_uid),
    dataset = x,
    formula = formula,
    feature_names = feature_names
  ) %>%
    (function(args) if (classification) rlang::modify(
      args, index_labels = index_labels
    ) else args)

  do.call(constructor, args)
}
