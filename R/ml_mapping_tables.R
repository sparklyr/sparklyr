ml_create_mapping_tables <- function() { # nocov start
  param_mapping_file <- system.file(file.path("sparkml", "param_mapping.json"), package = packageName())
  param_mapping_list <- jsonlite::fromJSON(param_mapping_file, simplifyVector = FALSE)

  param_mapping_r_to_s <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))
  param_mapping_s_to_r <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))

  invisible(lapply(names(param_mapping_list),
                   function(x) {
                     param_mapping_r_to_s[[x]] <- param_mapping_list[[x]]
                     param_mapping_s_to_r[[param_mapping_list[[x]]]] <- x
                   }))

  class_mapping_file <- system.file(file.path("sparkml", "class_mapping.json"), package = packageName())
  ml_class_mapping_list <- jsonlite::fromJSON(class_mapping_file, simplifyVector = FALSE)

  ml_class_mapping <- new.env(parent = emptyenv(),
                               size = length(ml_class_mapping_list))

  invisible(lapply(names(ml_class_mapping_list),
                   function(x) {
                     ml_class_mapping[[x]] <- ml_class_mapping_list[[x]]
                   }))

  rlang::ll(param_mapping_r_to_s = param_mapping_r_to_s,
            param_mapping_s_to_r = param_mapping_s_to_r,
            ml_class_mapping = ml_class_mapping)
} # nocov end
