# -------------------------- Feature Transformers ------------------------------

ft_process_step <- function(x, r_class, step_class, invoke_steps) {
  sc <- spark_connection(x)

  # Mapping R class to Spark model using /inst/sparkml/class_mapping.json
  class_mapping <- as.list(genv_get_ml_class_mapping())
  spark_class <- names(class_mapping[class_mapping == r_class])

  args <- list(sc, spark_class)

  jobj <- do.call(invoke_new, args)

  pe <- params_validate_estimator_and_set(jobj, invoke_steps)

  l_steps <- purrr::imap(pe, ~ list(.y, .x))

  for(i in seq_along(l_steps)) {
    if(!is.null(l_steps[[i]][[2]])) {
      jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
    }
  }

  new_transformer <- new_ml_transformer(jobj, class = step_class)

  post_ft_obj(
    x = x,
    transformer = new_transformer
  )
}

post_ft_obj <- function(x, transformer) {
  UseMethod("post_ft_obj")
}

post_ft_obj.spark_connection <- function(x, transformer) {
  transformer
}

post_ft_obj.ml_pipeline <- function(x, transformer) {
  ml_add_stage(x, transformer)
}

post_ft_obj.tbl_spark <- function(x, transformer){
  ml_transform(transformer, x)
}

# ------------------------------- Models ---------------------------------------

ml_process_model <- function(x, uid, r_class, invoke_steps, ml_function,
                             formula = NULL, response = NULL, features = NULL) {
  sc <- spark_connection(x)

  # Mapping R class to Spark model using /inst/sparkml/class_mapping.json
  class_mapping <- as.list(genv_get_ml_class_mapping())
  spark_class <- names(class_mapping[class_mapping == r_class])

  args <- list(sc, spark_class)
  if (!is.null(uid)) {
    uid <- cast_string(uid)
    args <- append(args, list(uid))
  }

  jobj <- do.call(invoke_new, args)

  pe <- params_validate_estimator_and_set(jobj, invoke_steps)

  l_steps <- purrr::imap(pe, ~ list(.y, .x))

  for(i in seq_along(l_steps)) {
    if(!is.null(l_steps[[i]][[2]])) {
      jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
    }
  }

  new_estimator <- new_ml_estimator(jobj, class = r_class)

  post_ml_obj(
    x = x,
    nm = new_estimator,
    ml_function = ml_function,
    formula = formula,
    response = response,
    features = features,
    features_col = invoke_steps$features_col,
    label_col = invoke_steps$label_col
  )

}

post_ml_obj <- function(x, nm, ml_function, formula, response,
                        features, features_col, label_col) {
  UseMethod("post_ml_obj")
}

post_ml_obj.spark_connection <- function(x, nm, ml_function, formula, response,
                                         features, features_col, label_col) {
  nm
}

post_ml_obj.ml_pipeline <- function(x, nm, ml_function, formula, response,
                                    features, features_col, label_col) {
  ml_add_stage(x, nm)
}

post_ml_obj.tbl_spark <- function(x, nm, ml_function, formula, response,
                                  features, features_col, label_col) {
  formula <- ml_standardize_formula(formula, response, features)

  if (is.null(formula)) {
    ml_fit(nm, x)
  } else {
    ml_construct_model_supervised(
      ml_function,
      predictor = nm,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col
    )
  }
}

# -------------------------------- Utils ---------------------------------------

param_min_version <- function(x, value, min_version = NULL, default = NULL) {
  res <- value
  if (!is.null(value)) {
    if (!is.null(min_version)) {
      sc <- spark_connection(x)
      ver <- spark_version(sc)
      if (ver < min_version) {
        if(value != default) {
          stop(paste0(
            "Parameter `", deparse(substitute(value)),
            "` is only available for Spark ", min_version, " and later.",
            "To avoid passing this variable, change the argument value to NULL."
          ))
        } else {
          res <- NULL
        }
      }
    }
  }
  res
}
