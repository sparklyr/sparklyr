ml_create_param_mapping_tables <- function() {
  param_mapping_list <-
    list("input_col" = "inputCol",
         "output_col" = "outputCol")

  # param_mapping_r_to_s <- new.env(hash = TRUE)
  param_mapping_s_to_r <- new.env(hash = TRUE, size = length(param_mapping_list))

  invisible(lapply(names(param_mapping_list),
                   function(x) {
                     # param_mapping_r_to_s[[x]] <- param_mapping_list[[x]]
                     param_mapping_s_to_r[[param_mapping_list[[x]]]] <- x
                   }))

  # devtools::use_data(param_mapping_r_to_s, internal = TRUE, overwrite = TRUE)
  devtools::use_data(param_mapping_s_to_r, internal = TRUE, overwrite = TRUE)
}
