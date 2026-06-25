#' Tidying methods for Spark ML ALS
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_als_tidiers
NULL

#' @rdname ml_als_tidiers
#' @export
tidy.ml_model_als <- function(x, ...) {
  user_factors <- x$model$user_factors %>%
    dplyr::select(!!"id", !!"features") %>%
    dplyr::rename(user_factors = !!"features")

  item_factors <- x$model$item_factors %>%
    dplyr::select(!!"id", !!"features") %>%
    dplyr::rename(item_factors = !!"features")

  dplyr::full_join(user_factors, item_factors, by = "id")
}

#' @rdname ml_als_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_als <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_als_tidiers
#' @export
glance.ml_model_als <- function(x, ...) {
  rank <- x$model$rank
  cold_start_strategy <- x$model$param_map$cold_start_strategy

  dplyr::tibble(
    rank = rank,
    cold_start_strategy = cold_start_strategy
  )
}

#' Tidying methods for Spark ML LDA models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_lda_tidiers
NULL

#' @rdname ml_lda_tidiers
#' @importFrom rlang !!
#' @export
tidy.ml_model_lda <- function(x, ...) {
  term <- ml_vocabulary(x)
  topics_matrix <- x$model$topics_matrix() %>%
    dplyr::as_tibble(.name_repair = "unique")

  k <- x$model$param_map$k
  names(topics_matrix) <- as.character(0:(k - 1))

  dplyr::bind_cols(term = term, topics_matrix) %>%
    tidyr::gather(!!"topic", beta, -term, convert = TRUE) %>%
    dplyr::select(!!"topic", term, beta)
}

#' @rdname ml_lda_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#' @importFrom rlang syms
#' @export
augment.ml_model_lda <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }

  vars <- c(dplyr::tbl_vars(newdata), "topicDistribution")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.topic = !!"topicDistribution")
}

#' @rdname ml_lda_tidiers
#' @export
glance.ml_model_lda <- function(x, ...) {
  k <- x$model$param_map$k
  vocab_size <- x$model$vocab_size
  learning_decay <- x$model$param_map$learning_decay
  optimizer <- x$model$param_map$optimizer

  dplyr::tibble(
    k = k,
    vocab_size = vocab_size,
    learning_decay = learning_decay,
    optimizer = optimizer
  )
}

#' Tidying methods for Spark ML tree models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_tree_tidiers
NULL

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_decision_tree_classification <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_decision_tree_regression <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_decision_tree_classification <- function(
  x,
  newdata = NULL,
  ...
) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_decision_tree_classification <- function(
  x,
  new_data = NULL,
  ...
) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_decision_tree_regression <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_decision_tree_regression <- function(
  x,
  new_data = NULL,
  ...
) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_decision_tree_classification <- function(x, ...) {
  glance_decision_tree(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_decision_tree_regression <- function(x, ...) {
  glance_decision_tree(x)
}

# glance() code for decision tree is the same
# for regression and classification
glance_decision_tree <- function(x) {
  num_nodes <- x$model$num_nodes()
  depth <- x$model$depth()
  impurity <- x$model$param_map$impurity

  dplyr::tibble(
    num_nodes = num_nodes,
    depth = depth,
    impurity = impurity
  )
}


#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_random_forest_classification <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_random_forest_regression <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_random_forest_classification <- function(
  x,
  newdata = NULL,
  ...
) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_random_forest_classification <- function(
  x,
  new_data = NULL,
  ...
) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_random_forest_regression <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_random_forest_regression <- function(
  x,
  new_data = NULL,
  ...
) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_random_forest_classification <- function(x, ...) {
  glance_random_forest(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_random_forest_regression <- function(x, ...) {
  glance_random_forest(x)
}

# glance() code for random forest the same
# for regression and classification
glance_random_forest <- function(x) {
  num_trees <- x$model$param_map$num_trees
  total_num_nodes <- x$model$total_num_nodes()
  max_depth <- x$model$param_map$max_depth
  impurity <- x$model$param_map$impurity
  subsampling_rate <- x$model$param_map$subsampling_rate

  dplyr::tibble(
    num_trees = num_trees,
    total_num_nodes = total_num_nodes,
    max_depth = max_depth,
    impurity = impurity,
    subsampling_rate = subsampling_rate
  )
}


#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_gbt_classification <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_gbt_regression <- function(x, ...) {
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gbt_classification <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_gbt_classification <- function(x, new_data = NULL, ...) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gbt_regression <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_gbt_regression <- function(x, new_data = NULL, ...) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_gbt_classification <- function(x, ...) {
  glance_gbt(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_gbt_regression <- function(x, ...) {
  glance_gbt(x)
}

# glance() code for gradient boosted trees is almost
#  the same for regression and classification
glance_gbt <- function(x) {
  # in gbt models, the total number of nodes is
  # model$total_num_nodes() in classification and
  # model$total_num_nodes in regression
  if (any(class(x) == "ml_model_gbt_regression")) {
    total_num_nodes <- x$model$total_num_nodes
  } else {
    total_num_nodes <- x$model$total_num_nodes()
  }

  num_trees <- length(x$model$trees())
  max_depth <- x$model$param_map$max_depth
  impurity <- x$model$param_map$impurity
  step_size <- x$model$param_map$step_size
  loss_type <- x$model$param_map$loss_type
  subsampling_rate <- x$model$param_map$subsampling_rate

  dplyr::tibble(
    num_trees = num_trees,
    total_num_nodes = total_num_nodes,
    max_depth = max_depth,
    impurity = impurity,
    step_size = step_size,
    loss_type = loss_type,
    subsampling_rate = subsampling_rate
  )
}

#' Tidying methods for Spark ML unsupervised models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_unsupervised_tidiers
NULL

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_kmeans <- function(x, ...) {
  center <- x$centers
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center, size = size, cluster = 0:(k - 1)) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_kmeans <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }
  vars <- c(dplyr::tbl_vars(newdata), "prediction")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.cluster = !!"prediction")
}

#' @rdname ml_unsupervised_tidiers
#' @export
glance.ml_model_kmeans <- function(x, ...) {
  k <- x$summary$k
  wssse <- compute_wssse(x)

  glance_tbl <- dplyr::tibble(
    k = k,
    wssse = wssse
  )

  add_silhouette(x, glance_tbl)
}

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_bisecting_kmeans <- function(x, ...) {
  center <- x$centers
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center, size = size, cluster = 0:(k - 1)) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_bisecting_kmeans <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }
  vars <- c(dplyr::tbl_vars(newdata), "prediction")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.cluster = !!"prediction")
}

#' @rdname ml_unsupervised_tidiers
#' @export
glance.ml_model_bisecting_kmeans <- function(x, ...) {
  k <- x$summary$k
  wssse <- compute_wssse(x)

  glance_tbl <- dplyr::tibble(
    k = k,
    wssse = wssse
  )

  add_silhouette(x, glance_tbl)
}

#' @rdname ml_unsupervised_tidiers
#' @importFrom dplyr .data
#' @export
tidy.ml_model_gaussian_mixture <- function(x, ...) {
  center <- x$gaussians_df()$mean %>%
    as.data.frame() %>%
    t() %>%
    fix_data_frame() %>%
    dplyr::select(-"term")

  names(center) <- x$feature_names

  weight <- x$weights
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center, weight = weight, size = size, cluster = 0:(k - 1)) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gaussian_mixture <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }
  vars <- c(dplyr::tbl_vars(newdata), "prediction")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.cluster = !!"prediction")
}

#' @rdname ml_unsupervised_tidiers
#' @export
glance.ml_model_gaussian_mixture <- function(x, ...) {
  k <- x$summary$k
  glance_tbl <- dplyr::tibble(k = k)
  add_silhouette(x, glance_tbl)
}

# this function add silhouette to glance if
# spark version is even or greater than 2.3.0
add_silhouette <- function(x, glance_tbl) {
  sc <- spark_connection(x$dataset)
  version <- spark_version(sc)

  if (version >= "2.3.0") {
    silhouette <- ml_clustering_evaluator(x$summary$predictions)
    glance_tbl <- dplyr::bind_cols(glance_tbl, silhouette = silhouette)
  }
  glance_tbl
}

compute_wssse <- function(x) {
  if (is_required_spark(spark_connection(x$dataset), "3.0.0")) {
    wssse <- x$summary$training_cost
  } else {
    wssse <- x$cost
  }
  wssse
}

#' Tidying methods for Spark ML Principal Component Analysis
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_pca_tidiers
NULL

#' @rdname ml_pca_tidiers
#' @export
tidy.ml_model_pca <- function(x, ...) {
  dplyr::as_tibble(x$pc, rownames = "features")
}

#' @rdname ml_pca_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_pca <- function(x, newdata = NULL, ...) {
  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }

  sdf_project(x, newdata)
}

#' @rdname ml_pca_tidiers
#' @export
glance.ml_model_pca <- function(x, ...) {
  explained_variance <- x$explained_variance
  names(explained_variance) <- purrr::map_chr(
    names(explained_variance),
    function(e) paste0("explained_variance_", e)
  )

  k <- c("k" = x$k)

  c(k, explained_variance) %>%
    as.list() %>%
    dplyr::as_tibble()
}
