rsApiUpdateDialog <- function(code) {
  if (exists(".rs.updateNewConnectionDialog")) {
    updateDialog <- get(".rs.updateNewConnectionDialog")
    updateDialog(code)
  }
}

rsApiShowDialog <- function(title, message) {
  if (exists(".rs.showDialog")) {
    showDialog <- get(".rs.showDialog")
    showDialog(title, message)
  }
}

rsApiShowPrompt <- function(title, message, default) {
  if (exists(".rs.showPrompt")) {
    showPrompt <- get(".rs.showPrompt")
    showPrompt(title, message, default)
  }
}

#' @import shiny
connection_spark_ui <- function() {
  componentVersionSelectChoices <- function(name) {
    selected <- spark_default_version()[[name]]
    choiceValues <- unique(spark_installed_versions()[[name]])
    choiceNames <- choiceValues
    choiceNames <- lapply(
      choiceNames,
      function(e) if (e == selected) paste(e, "(Default)") else e
    )

    names(choiceValues) <- choiceNames

    choiceValues
  }

  tags$div(
    tags$head(
      tags$style(
        HTML("
          body {
            background: none;

            font-family : \"Lucida Sans\", \"DejaVu Sans\", \"Lucida Grande\", \"Segoe UI\", Verdana, Helvetica, sans-serif;
            font-size : 12px;
            -ms-user-select : none;
            -moz-user-select : none;
            -webkit-user-select : none;
            user-select : none;

            margin: 0;
            margin-top: 5px;
          }

          .shiny-input-container {
            min-width: 100%;
            margin-bottom: 7px;
          }

          .shiny-input-container > .control-label {
            display: inline-block;
            width: 195px;
          }

          .shiny-input-container > div {
            display: inline-block;
            width: 300px;
          }

          #shiny-disconnected-overlay {
            display: none;
          }
        ")
      )
    ),
    selectInput(
      "master",
      "Master:",
      choices = c(
        "local" = "local",
        "Cluster..." = "cluster"
        # TODO: Changing spark versions filters the right hadoop version
        # TODO: If Spark not installed, prompt install
        # TODO: If java not installed: ComponentsNotInstalledDialogs.showJavaNotInstalled(context.getJavaInstallUrl());
        # TODO: Selection opens "Connect to Cluster", "Spark master: ". "spark://local:7077"
        # TODO: Support rstudio.spark.connections option
        # TODO: Need to store dialog preferences somwhere (say, selecting dplyr) (see connectionsDbInterface)
      ),
      selectize = FALSE
    ),
    selectInput(
      "dbinterface",
      "DB Interface:",
      choices = c(
        "dplyr" = "dplyr",
        "(None)" = "none"
      ),
      selectize = FALSE
    ),
    div(
      style = "height: 10px"
    ),
    selectInput(
      "sparkversion",
      "Spark version:",
      choices = componentVersionSelectChoices("spark"),
      selected = spark_default_version()$spark,
      selectize = FALSE
    ),
    selectInput(
      "hadoopversion",
      "Hadoop version:",
      choices = componentVersionSelectChoices("hadoop"),
      selected = spark_default_version()$hadoop,
      selectize = FALSE
    )
  )
}

connection_spark_server <- function(input, output, session) {
  hasDefaultSparkVersion <- reactive({
    input$sparkversion == spark_default_version()$spark
  })

  hasDefaultHadoopVersion <- reactive({
    input$hadoopversion == spark_default_version()$hadoop
  })

  codeReactive <- reactive({
    paste(
      "library(sparklyr)\n",
      if(input$dbinterface == "dplyr") "library(dplyr)\n" else "",
      "sc ",
      "<- ",
      "spark_connect(master = \"",
      input$master,
      "\"",
      if (!hasDefaultSparkVersion()) paste(", version = \"", input$sparkversion, "\"", sep = "") else "",
      if (!hasDefaultHadoopVersion()) paste(", hadoop_version = \"", input$hadoopversion, "\"", sep = "") else "",
      ")",
      sep = ""
    )
  })

  observe({
    rsApiUpdateDialog(codeReactive())
  })

  observe({
    if (identical(input$master, "cluster")) {
      if (identical(rstudioapi::versionInfo()$mode, "desktop")) {
        rsApiShowDialog(
          "Connect to Spark",
          paste(
            "Connecting with a remote Spark cluster requires ",
            "an RStudio Server instance that is either within the cluster ",
            "or has a high bandwidth connection to the cluster.</p>",
            "<p>Please see the <strong>Using Spark with RStudio</strong> help ",
            "link below for additional details.</p>",
            sep = ""
          )
        )

        updateSelectInput(
          session,
          "master",
          selected = "local"
        )
      }
      else if (identical(spark_home(), NULL)) {
        rsApiShowDialog(
          "Connect to Spark",
          paste(
            "<p>Connecting with a Spark cluster requires that you are on a system ",
            "able to communicate with the cluster in both directions, and ",
            "requires that the SPARK_HOME environment variable refers to a  ",
            "locally installed version of Spark that is configured to ",
            "communicate with the cluster.</p>",
            "<p>Your system doesn't currently have the SPARK_HOME environment ",
            "variable defined. Please contact your system administrator to ",
            "ensure that the server is properly configured to connect with ",
            "the cluster.<p>",
            sep = ""
          )
        )

        updateSelectInput(
          session,
          "master",
          selected = "local"
        )
      }
      else {
        master <- rsApiShowPrompt(
          "Connect to Cluster",
          "Spark master:",
          "yarn-client"
        )

        updateSelectInput(
          session,
          "master",
          choices = list(master = "master", master, "Cluster..." = "cluster"),
          selected = master
        )
      }
    }
  })
}

#' A Shiny app that can be used to construct a \code{spark_connect} statement
#'
#' @export
#'
#' @import shiny
#'
#' @keywords internal
connection_spark_shinyapp <- function() {
  shinyApp(connection_spark_ui, connection_spark_server)
}
