spark_yarn_cluster_get_gateway <- function() {
  confDir <- Sys.getenv("YARN_CONF_DIR")
  if (nchar(confDir) == 0) {

    # some systems don't set YARN_CONF_DIR but do set HADOOP_CONF_DIR
    confDir <- Sys.getenv("HADOOP_CONF_DIR")
    if (nchar(confDir) == 0) {
      stop("Yarn Cluster mode requires YARN_CONF_DIR or HADOOP_CONF_DIR to be set.")
    }
  }

  yarnSite <- file.path(confDir, "yarn-site.xml")
  if (!file.exists(yarnSite)) {
    stop("Yarn Cluster mode requires yarn-site.xml to exist under YARN_CONF_DIR")
  }

  yarnSiteXml <- xml2::read_xml(yarnSite)

  yarnResourceManagerAddress <- xml2::xml_text(xml2::xml_find_all(
    yarnSiteXml,
    "//name[.='yarn.resourcemanager.address']/parent::property/value")
  )

  if (length(yarnResourceManagerAddress) == 0) {
    stop("Yarn Cluster mode uses `yarn.resourcemanager.address` but is not present in yarn-site.xml")
  }

  strsplit(yarnResourceManagerAddress, ":")[[1]][[1]]
}
