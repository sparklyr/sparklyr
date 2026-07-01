#' Constructors for `ml_model` Objects
#'
#' Functions for developers writing extensions for Spark ML. These functions are constructors
#'   for `ml_model` objects that are returned when using the formula interface.
#'
#' @name ml-model-constructors
#'
#' @param pipeline_model The pipeline model object returned by `ml_supervised_pipeline()`.
#' @param dataset The training dataset.
#' @template roxlate-ml-label-col
#' @template roxlate-ml-features-col
#' @param class Name of the subclass.
#' @param predictor The pipeline stage corresponding to the ML algorithm.
#' @param formula The formula used for data preprocessing
#' @keywords internal
NULL

#' @export
#' @rdname ml-model-constructors
new_ml_model_prediction <- function(
  pipeline_model,
  formula,
  dataset,
  label_col,
  features_col,
  ...,
  class = character()
) {
  feature_names <- ml_feature_names_metadata(
    pipeline_model,
    dataset,
    features_col
  )
  response <- gsub("~.+$", "", formula) %>% trimws()

  new_ml_model(
    pipeline_model,
    formula = formula,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    feature_names = feature_names,
    response = response,
    ...,
    class = c(class, "ml_model_prediction")
  )
}

#' @export
#' @rdname ml-model-constructors
new_ml_model <- function(
  pipeline_model,
  formula,
  dataset,
  ...,
  class = character()
) {
  sc <- spark_connection(pipeline_model)

  stages <- ml_stages(pipeline_model)
  predictor <- stages[[length(stages)]]

  # for pipeline, fix data prep transformation but use the un-fitted estimator predictor
  pipeline <- stages %>% head(-1)
  pipeline <- rlang::exec(
    ml_pipeline,
    !!!pipeline,
    uid = ml_uid(pipeline_model)
  ) %>%
    ml_add_stage(predictor)

  # workaround for https://issues.apache.org/jira/browse/SPARK-19953
  model_uid <- if (spark_version(sc) < "2.2.0") {
    switch(
      class(predictor)[[1]],
      ml_random_forest_regressor = "rfr",
      ml_random_forest_classifier = "rfc",
      ml_uid(predictor)
    )
  } else {
    ml_uid(predictor)
  }

  model <- ml_stage(pipeline_model, model_uid)

  structure(
    list(
      pipeline_model = pipeline_model,
      formula = formula,
      dataset = dataset,
      pipeline = pipeline,
      model = model,
      ...
    ),
    class = c(class, "ml_model")
  )
}

#' @export
#' @rdname ml-model-constructors
new_ml_model_classification <- function(
  pipeline_model,
  formula,
  dataset,
  label_col,
  features_col,
  predicted_label_col,
  ...,
  class = character()
) {
  m <- new_ml_model_prediction(
    pipeline_model,
    formula = formula,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    predicted_label_col = predicted_label_col,
    ...,
    class = c(class, "ml_model_classification")
  )

  label_indexer_model <- ml_stages(pipeline_model) %>%
    dplyr::nth(-2) # second from last, either RFormulaModel or StringIndexerModel
  index_labels <- ml_index_labels_metadata(
    label_indexer_model,
    dataset,
    label_col
  )

  if (!is.null(index_labels)) {
    sc <- spark_connection(pipeline_model)
    index_to_string <- ft_index_to_string(
      sc,
      ml_param(m$model, "prediction_col"),
      predicted_label_col,
      index_labels
    )
    m$pipeline <- m$pipeline %>%
      ml_add_stage(index_to_string)
    m$pipeline_model <- m$pipeline_model %>%
      ml_add_stage(index_to_string) %>%
      # ml_fit() here doesn't do any actual computation but simply
      #   returns a PipelineModel since ml_add_stage() returns a
      #   Pipeline (Estimator)
      ml_fit(dataset)
    m$index_labels <- index_labels
  }

  m
}

#' @export
#' @rdname ml-model-constructors
new_ml_model_regression <- function(
  pipeline_model,
  formula,
  dataset,
  label_col,
  features_col,
  ...,
  class = character()
) {
  new_ml_model_prediction(
    pipeline_model,
    formula,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    ...,
    class = c(class, "ml_model_regression")
  )
}

#' @export
#' @rdname ml-model-constructors
new_ml_model_clustering <- function(
  pipeline_model,
  formula,
  dataset,
  features_col,
  ...,
  class = character()
) {
  predictor <- dplyr::last(pipeline_model$stages)

  # LDA uses more than one preprocessor and ml_feature_names_metadata()
  # considers just one: ml_stage(pipeline_model, 1)
  if (inherits(predictor, "ml_lda_model")) {
    feature_names <- gsub("~", "", formula) # LDA uses just one feature
  } else {
    feature_names <- ml_feature_names_metadata(
      pipeline_model,
      dataset,
      features_col
    )
  }

  new_ml_model(
    pipeline_model,
    formula,
    dataset = dataset,
    features_col = features_col,
    feature_names = feature_names,
    ...,
    class = c(class, "ml_model_clustering")
  )
}

new_ml_model_recommendation <- function(
  pipeline_model,
  formula,
  dataset,
  ...,
  class = character()
) {
  new_ml_model(
    pipeline_model,
    formula,
    dataset = dataset,
    ...,
    class = c(class, "ml_model_recommendation")
  )
}

#' @export
spark_jobj.ml_model <- function(x, ...) {
  spark_jobj(x$pipeline_model)
}

#' @export
print.ml_model <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat(invoke(spark_jobj(x$model), "toString"), sep = "\n")
}

#' @export
#' @rdname ml-model-constructors
ml_supervised_pipeline <- function(
  predictor,
  dataset,
  formula,
  features_col,
  label_col
) {
  sc <- spark_connection(predictor)
  r_formula <- ft_r_formula(sc, formula, features_col, label_col)
  pipeline_model <- ml_pipeline(r_formula, predictor) %>%
    ml_fit(dataset)
}

#' @export
#' @rdname ml-model-constructors
ml_clustering_pipeline <- function(predictor, dataset, formula, features_col) {
  sc <- spark_connection(predictor)

  r_formula <- ft_r_formula(
    sc,
    formula = formula,
    features_col = features_col
  )
  pipeline <- ml_pipeline(r_formula, predictor)

  pipeline %>% ml_fit(dataset)
}

ml_recommendation_pipeline <- function(predictor, dataset, formula) {
  sc <- spark_connection(predictor)
  r_formula <- ft_r_formula(sc, formula)
  pipeline_model <- ml_pipeline(r_formula, predictor) %>%
    ml_fit(dataset)
}

#' @export
#' @rdname ml-model-constructors
#' @param constructor The constructor function for the `ml_model`.
ml_construct_model_supervised <- function(
  constructor,
  predictor,
  formula,
  dataset,
  features_col,
  label_col,
  ...
) {
  pipeline_model <- ml_supervised_pipeline(
    predictor = predictor,
    dataset = dataset,
    formula = formula,
    features_col = features_col,
    label_col = label_col
  )

  .args <- list(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    label_col = label_col,
    ...
  )

  rlang::exec(constructor, !!!.args)
}

#' @export
#' @rdname ml-model-constructors
ml_construct_model_clustering <- function(
  constructor,
  predictor,
  formula,
  dataset,
  features_col,
  ...
) {
  pipeline_model <- ml_clustering_pipeline(
    predictor = predictor,
    dataset = dataset,
    formula = formula,
    features_col = features_col
  )

  .args <- list(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    ...
  )

  rlang::exec(constructor, !!!.args)
}

ml_construct_model_recommendation <- function(
  constructor,
  predictor,
  formula,
  dataset,
  ...
) {
  pipeline_model <- ml_recommendation_pipeline(
    predictor = predictor,
    dataset = dataset,
    formula = formula
  )

  .args <- list(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    ...
  )

  rlang::exec(constructor, !!!.args)
}

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
  } else {
    # vector assembler
    ml_param(preprocessor, "output_col")
  }

  ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(!!rlang::sym("idx")) %>%
    dplyr::pull("name")
}
