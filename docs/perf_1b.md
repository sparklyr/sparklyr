RSpark Performance: 1B Rows
================

Spark 1.0
---------

    spark.conf.set("spark.sql.codegen.wholeStage", false)

    benchmark("Spark 1.6") {
      spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
    }

Spark 2.0
---------

    spark.conf.set("spark.sql.codegen.wholeStage", true)

    spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
