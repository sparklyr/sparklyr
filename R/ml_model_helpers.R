#' @export
#' @rdname ml-model-constructors
ml_supervised_pipeline <- function(predictor, dataset, formula, features_col, label_col) {
  sc <- spark_connection(predictor)
  r_formula <- ft_r_formula(sc, formula, features_col, label_col)
  pipeline_model <- ml_pipeline(r_formula, predictor) %>%
    ml_fit(dataset)
}

#' @export
#' @rdname ml-model-constructors
ml_clustering_pipeline <- function(predictor, dataset, formula, features_col) {
  sc <- spark_connection(predictor)

  pipeline <- if (spark_version(sc) < "2.0.0") {
    rdf <- sdf_schema(dataset) %>%
      lapply(`[[`, "name") %>%
      as.data.frame(stringsAsFactors = FALSE)
    features <- stats::terms(as.formula(formula), data = rdf) %>%
      attr("term.labels")

    vector_assembler <- ft_vector_assembler(
      sc,
      input_cols = features, output_col = features_col
    )
    ml_pipeline(vector_assembler, predictor)
  } else {
    r_formula <- ft_r_formula(sc, formula = formula, features_col = features_col)
    ml_pipeline(r_formula, predictor)
  }

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
ml_construct_model_supervised <- function(constructor, predictor, formula, dataset,
                                          features_col, label_col, ...) {
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
ml_construct_model_clustering <- function(constructor, predictor, formula, dataset, features_col, ...) {
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

ml_construct_model_recommendation <- function(constructor, predictor, formula,
                                              dataset, ...) {
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
