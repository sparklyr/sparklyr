spark_yarn_cluster_get_gateway <- function() {
  hadoopConfDir <- Sys.getenv("YARN_CONF_DIR")
  if (nchar(hadoopConfDir) == 0) {
    stop("Yarn Cluster mode requires YARN_CONF_DIR to be set.")
  }

  yarnSite <- file.path(hadoopConfDir, "yarn-site.xml")
  if (!file.exists(yarnSite)) {
    stop("Yarn Cluster mode requires yarn-site.xml to exist under YARN_CONF_DIR")
  }

  yarnSiteXml <- xml2::read_xml(yarnSite)

  yarnResourceManagerAddress <- xml2::xml_text(xml2::xml_find_all(
    yarnSiteXml,
    "//name[.='yarn.resourcemanager.address']/parent::property/value")
  )

  if (length(yarnResourceManagerAddress) == 0) {
    stop("Yarn Cluster mode uses `yarn.resourcemanager.address` but is not present in yarn-size.xml")
  }

  strsplit(yarnResourceManagerAddress, ":")[[1]][[1]]
}
