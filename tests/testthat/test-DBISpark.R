
spark_install()
scon <- spark_connect("local")
ctx <- DBItest::make_context(DBISpark(scon), NULL)

DBItest::test_getting_started(ctx = ctx, skip = c("package_name"))

# DBItest::test_driver()
# DBItest::test_connection()
# DBItest::test_result()
# DBItest::test_sql()
# DBItest::test_meta()
# DBItest::test_compliance()
