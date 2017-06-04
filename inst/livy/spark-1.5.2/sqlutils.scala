//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.r.SerDe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.util.matching.Regex

object SQLUtils {
  def createStructType(fields : Seq[StructField]): StructType = {
    StructType(fields)
  }

  def getSQLDataType(dataType: String): DataType = {
    val RegexArray = "\\Aarray<(.+)>\\Z".r("elemType")
    val RegexMap = "\\Amap<(.+),(.+)>\\Z".r("keyType", "valueType")
    val RegexStruct = "\\Astruct<(.+)>\\Z".r("fieldsStr")
    val RegexStructField = "\\A(.+):(.+)\\Z".r("fieldName", "fieldType")

    dataType match {
      case "byte" => org.apache.spark.sql.types.ByteType
      case "integer" => org.apache.spark.sql.types.IntegerType
      case "float" => org.apache.spark.sql.types.FloatType
      case "double" => org.apache.spark.sql.types.DoubleType
      case "numeric" => org.apache.spark.sql.types.DoubleType
      case "character" => org.apache.spark.sql.types.StringType
      case "string" => org.apache.spark.sql.types.StringType
      case "binary" => org.apache.spark.sql.types.BinaryType
      case "raw" => org.apache.spark.sql.types.BinaryType
      case "logical" => org.apache.spark.sql.types.BooleanType
      case "boolean" => org.apache.spark.sql.types.BooleanType
      case "timestamp" => org.apache.spark.sql.types.TimestampType
      case "date" => org.apache.spark.sql.types.DateType
      case RegexArray(elemType) =>
        org.apache.spark.sql.types.ArrayType(getSQLDataType(elemType))
      case RegexMap(keyType, valueType) =>
        if (keyType != "string" && keyType != "character") {
          throw new IllegalArgumentException("Key type of a map must be string or character")
        }
        org.apache.spark.sql.types.MapType(getSQLDataType(keyType), getSQLDataType(valueType))
      case RegexStruct(fieldsStr) =>
        if (fieldsStr(fieldsStr.length - 1) == ',') {
          throw new IllegalArgumentException(s"Invaid type $dataType")
        }
        val fields = fieldsStr.split(",")
        val structFields = fields.map { field =>
          field match {
            case RegexStructField(fieldName, fieldType) =>
              createStructField(fieldName, fieldType, true)

            case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
          }
        }
        createStructType(structFields)
      case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
    }
  }

  def createStructField(name: String, dataType: String, nullable: Boolean): StructField = {
    val dtObj = getSQLDataType(dataType)
    StructField(name, dtObj, nullable)
  }
}
