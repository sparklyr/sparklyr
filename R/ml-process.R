# Sibling of `ml_process_model()` for the `ft_*` feature functions. Feature stages
# have no formula and no supervised-model constructor; they end in a transform (or
# fit-and-transform) rather than building an `ml_model_*` object. See
# `.github/planning/ml_process_feature.md` for the design rationale.

ml_process_feature <- function(
  x,
  uid,
  r_class,
  invoke_steps,
  stage_constructor
) {
  sc <- spark_connection(x)

  # Mapping R class to Spark feature stage using /inst/sparkml/class_mapping.json
  class_mapping <- as.list(genv_get_ml_class_mapping())
  spark_class <- names(class_mapping[class_mapping == r_class])

  if (length(spark_class) != 1) {
    stop(
      "Could not resolve a unique Spark class for r_class '",
      r_class,
      "' (found ",
      length(spark_class),
      " match(es) in the class mapping)."
    )
  }

  args <- list(sc, spark_class)
  if (!is.null(uid)) {
    uid <- cast_string(uid)
    args <- append(args, list(uid))
  }

  jobj <- do.call(invoke_new, args)

  pe <- params_validate_estimator_and_set(jobj, invoke_steps)

  l_steps <- purrr::imap(pe, ~ list(.y, .x))

  for (i in seq_along(l_steps)) {
    if (!is.null(l_steps[[i]][[2]])) {
      jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
    }
  }

  # The passed-in `new_ml_*()` is the same constructor reflection uses, so forward
  # construction and reverse reflection share one source of truth for the S3 class
  # and the transformer-vs-estimator kind (see Gotcha A in the scoping doc).
  nm <- stage_constructor(jobj)

  post_ft_obj(x, nm)
}

# --------------------- Class-aware param validators ---------------------------

# Per-transformer scalar params live in class-aware overrides rather than the
# shared params_base_validator(), so a future estimator that reuses one of these
# names with different semantics is unaffected. Each starts from the base and adds
# only its own scalars (see Gotcha E in the scoping doc).

#' @export
params_validator.ml_standard_scaler <- function(x) {
  v <- params_base_validator(x)
  v[["with_mean"]] <- function(x) cast_scalar_logical(x)
  v[["with_std"]] <- function(x) cast_scalar_logical(x)
  v
}

#' @export
params_validator.ml_dct <- function(x) {
  v <- params_base_validator(x)
  v[["inverse"]] <- function(x) cast_scalar_logical(x)
  v
}

#' @export
params_validator.ml_ngram <- function(x) {
  v <- params_base_validator(x)
  v[["n"]] <- function(x) cast_scalar_integer(x)
  v
}

#' @export
params_validator.ml_normalizer <- function(x) {
  v <- params_base_validator(x)
  v[["p"]] <- function(x) {
    x <- cast_scalar_double(x)
    if (x < 1) {
      stop("`p` must be at least 1.")
    }
    x
  }
  v
}

#' @export
params_validator.ml_polynomial_expansion <- function(x) {
  v <- params_base_validator(x)
  v[["degree"]] <- function(x) {
    x <- cast_scalar_integer(x)
    if (x < 1) {
      stop("`degree` must be at least 1.", call. = FALSE)
    }
    x
  }
  v
}

#' @export
params_validator.ml_regex_tokenizer <- function(x) {
  v <- params_base_validator(x)
  v[["gaps"]] <- function(x) cast_scalar_logical(x)
  v[["min_token_length"]] <- function(x) cast_scalar_integer(x)
  v[["pattern"]] <- function(x) cast_string(x)
  v[["to_lower_case"]] <- function(x) cast_scalar_logical(x)
  v
}

#' @export
params_validator.ml_min_max_scaler <- function(x) {
  v <- params_base_validator(x)
  v[["min"]] <- function(x) cast_scalar_double(x)
  v[["max"]] <- function(x) cast_scalar_double(x)
  v
}

#' @export
params_validator.ml_robust_scaler <- function(x) {
  v <- params_base_validator(x)
  v[["lower"]] <- function(x) cast_scalar_double(x)
  v[["upper"]] <- function(x) cast_scalar_double(x)
  v[["with_centering"]] <- function(x) cast_scalar_logical(x)
  v[["with_scaling"]] <- function(x) cast_scalar_logical(x)
  v[["relative_error"]] <- function(x) cast_scalar_double(x)
  v
}

#' @export
params_validator.ml_idf <- function(x) {
  v <- params_base_validator(x)
  v[["min_doc_freq"]] <- function(x) cast_scalar_integer(x)
  v
}

#' @export
params_validator.ml_pca <- function(x) {
  v <- params_base_validator(x)
  v[["k"]] <- function(x) cast_nullable_scalar_integer(x)
  v
}

# --------------------- Post conversion functions ------------------------------

post_ft_obj <- function(x, nm) {
  UseMethod("post_ft_obj")
}

#' @export
post_ft_obj.spark_connection <- function(x, nm) {
  nm
}

#' @export
post_ft_obj.ml_pipeline <- function(x, nm) {
  ml_add_stage(x, nm)
}

#' @export
post_ft_obj.tbl_spark <- function(x, nm) {
  # `ml_transform` requires an `ml_transformer`; `ml_fit_and_transform` requires an
  # `ml_estimator`. The wrapped object already carries the right class, so the
  # branch needs no `ml_function`: a fitted estimator is wrapped back to its
  # `*_model` R class by the existing `ml_call_constructor()` reflection.
  if (is_ml_transformer(nm)) {
    ml_transform(nm, x)
  } else {
    ml_fit_and_transform(nm, x)
  }
}
