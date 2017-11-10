#' Feature Tranformation -- RFormula (Estimator)
#'
#' Implements the transforms required for fitting a dataset against an R model
#'   formula. Currently we support a limited subset of the R operators,
#'   including \code{~}, \code{.}, \code{:}, \code{+}, and \code{-}. Also see the R formula docs here:
#'   \url{http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html}
#'
#' @details The basic operators in the formula are:
#'
#'   \itemize{
#'     \item ~ separate target and terms
#'     \item + concat terms, "+ 0" means removing intercept
#'     \item - remove a term, "- 1" means removing intercept
#'     \item : interaction (multiplication for numeric values, or binarized categorical values)
#'     \item . all columns except target
#'   }
#'
#'   Suppose a and b are double columns, we use the following simple examples to illustrate the
#'   effect of RFormula:
#'
#'   \itemize{
#'     \item \code{y ~ a + b} means model \code{y ~ w0 + w1 * a + w2 * b}
#'       where \code{w0} is the intercept and \code{w1, w2} are coefficients.
#'     \item \code{y ~ a + b + a:b - 1} means model \code{y ~ w1 * a + w2 * b + w3 * a * b}
#'       where \code{w1, w2, w3} are coefficients.
#'   }
#'
#'  RFormula produces a vector column of features and a double or string column
#'  of label. Like when formulas are used in R for linear regression, string
#'  input columns will be one-hot encoded, and numeric columns will be cast to
#'  doubles. If the label column is of type string, it will be first transformed
#'  to double with StringIndexer. If the label column does not exist in the
#'  DataFrame, the output label column will be created from the specified
#'  response variable in the formula.
#'
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-features-col
#' @template roxlate-ml-label-col
#'
#' @param formula R formula as a character string or a formula. Formula objects are
#'   converted to character strings directly and the environment is not captured.
#' @param force_index_label (Spark 2.1.0+) Force to index label whether it is numeric or
#'   string type. Usually we index label only when it is string type. If
#'   the formula was used by classification algorithms, we can force to index
#'   label even it is numeric type by setting this param with true.
#'   Default: \code{FALSE}.
#'
#' @export
ft_r_formula <- function(x, formula, features_col = "features", label_col = "label",
                         force_index_label = FALSE, dataset = NULL,
                         uid = random_string("r_formula_"), ...) {
  UseMethod("ft_r_formula")
}

#' @export
ft_r_formula.spark_connection <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...) {

  ml_ratify_args()

  estimator <- invoke_new(x, "org.apache.spark.ml.feature.RFormula", uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setFormula", formula) %>%
    invoke("setLabelCol", label_col) %>%
    jobj_set_param("setForceIndexLabel", force_index_label, FALSE, "2.1.0") %>%
    new_ml_r_formula()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_r_formula.ml_pipeline <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_r_formula.tbl_spark <- function(
  x, formula, features_col = "features", label_col = "label",
  force_index_label = FALSE, dataset = NULL,
  uid = random_string("r_formula_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_r_formula <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_r_formula")
}

new_ml_r_formula_model <- function(jobj) {
  new_ml_transformer(jobj,
                     formula = try_null(jobj %>%
                                          invoke("parent") %>%
                                          invoke("getFormula")),
                     subclass = "ml_r_formula_model")
}

# Validator

ml_validator_r_formula <- function(args, nms) {
  args %>%
    ml_validate_args({
      if (rlang::is_formula(formula))
        formula <- rlang::expr_text(args$formula, width = 500L)
      formula <- ensure_scalar_character(formula)
      features_col <- ensure_scalar_character(features_col)
      label_col <- ensure_scalar_character(label_col)
      force_index_label <- ensure_scalar_boolean(force_index_label)
    }, list(sql = "statement")) %>%
    ml_extract_args(nms, list(sql = "statement"))
}
