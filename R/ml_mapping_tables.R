register_mapping_tables <- function() {

  flip_named_list <- function(x) stats::setNames(as.list(names(x)), unlist(x))
  create_env_from_mappings <- function(x) purrr::reduce(
    purrr::map(x, ~ list2env(.x, parent = emptyenv())), rlang::env_poke_parent
  )

  read_extension_mappings <- function(file_name) {
    pkgs <- registered_extensions()
    mappings <- pkgs %>%
      purrr::map_chr(function(pkg) {
        if (nzchar(devtools_file <- system.file("inst", "sparkml", file_name, package = pkg))) {
          return(devtools_file)
        }
        system.file("sparkml", file_name, package = pkg)
      }) %>%
      purrr::set_names(pkgs) %>%
      purrr::keep(nzchar) %>%
      purrr::map(jsonlite::fromJSON)

    if (length(mappings)) mappings else NULL
  }

  read_base_mapping <- function(file_name) {
    system.file("sparkml", file_name, package = "sparklyr") %>%
      jsonlite::fromJSON()
  }

  param_mapping_r_to_s <- read_base_mapping("param_mapping.json") %>%
    flip_named_list() %>%
    as.environment()

  param_mapping_s_to_r <- read_base_mapping("param_mapping.json") %>%
    as.environment()

  extension_param_mappings <- read_extension_mappings(file_name = "param_mapping.json")

  if (!is.null(extension_param_mappings)) {
    extension_param_mappings_r_to_s <- extension_param_mappings %>%
      purrr::map(flip_named_list) %>%
      create_env_from_mappings()

    extension_param_mappings_s_to_r <- extension_param_mappings %>%
      create_env_from_mappings()

    rlang::env_poke_parent(param_mapping_r_to_s, extension_param_mappings_r_to_s)
    rlang::env_poke_parent(param_mapping_s_to_r, extension_param_mappings_s_to_r)
  }

  .globals$param_mapping_r_to_s <- param_mapping_r_to_s
  .globals$param_mapping_s_to_r <- param_mapping_s_to_r

  ml_class_mapping <- read_base_mapping(file_name = "class_mapping.json") %>%
    as.environment()

  extension_class_mappings <- read_extension_mappings(file_name = "class_mapping.json")

  ml_package_mapping <- read_base_mapping(file_name = "class_mapping.json") %>%
    purrr::map(~ "sparklyr") %>%
    as.environment()

  extension_package_mappings <- read_extension_mappings(file_name = "class_mapping.json") %>%
    purrr::imap(function(l, pkg) purrr::map(l, ~ pkg))



  if (!is.null(extension_class_mappings)) {
    extension_class_mappings <- create_env_from_mappings(extension_class_mappings)
    rlang::env_poke_parent(ml_class_mapping, extension_class_mappings)

    extension_package_mappings <- create_env_from_mappings(extension_package_mappings)
    rlang::env_poke_parent(ml_package_mapping, extension_package_mappings)
  }

  .globals$ml_class_mapping <- ml_class_mapping
  .globals$ml_package_mapping <- ml_package_mapping
}
