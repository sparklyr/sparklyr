package sparklyr

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
      case "BooleanType" => BooleanType
      case "ByteType" => ByteType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "FloatType" => FloatType
      case "DoubleType" => DoubleType
      case "StringType" => StringType
      case "TimestampType" => TimestampType
      case "DateType" => DateType
      case "BinaryType" => BinaryType
      case "byte" => ByteType
      case "integer" => IntegerType
      case "integer64" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "numeric" => DoubleType
      case "long" => LongType
      case "character" => StringType
      case "factor" => StringType
      case "string" => StringType
      case "binary" => BinaryType
      case "raw" => BinaryType
      case "logical" => BooleanType
      case "boolean" => BooleanType
      case "POSIXct" => TimestampType
      case "POSIXlt" => TimestampType
      case "timestamp" => TimestampType
      case "Date" => DateType
      case "date" => DateType
      case "spark_apply_binary_result" => ArrayType(BinaryType)
      case RegexArray(elemType) =>
        ArrayType(getSQLDataType(elemType))
      case RegexMap(keyType, valueType) =>
        if (keyType != "string" && keyType != "character") {
          throw new IllegalArgumentException("Key type of a map must be string or character")
        }
        MapType(getSQLDataType(keyType), getSQLDataType(valueType))
      case RegexStruct(fieldsStr) =>
        if (fieldsStr(fieldsStr.length - 1) == ',') {
          throw new IllegalArgumentException(s"Invalid type $dataType")
        }
        val fields = fieldsStr.split(",")
        val structFields = fields.map { field =>
          field match {
            case RegexStructField(fieldName, fieldType) =>
              createStructField(fieldName, fieldType, true)

            case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
          }
        }
        createStructType(structFields)
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
    }
  }

  def createStructField(name: String, dataType: String, nullable: Boolean): StructField = {
    val dtObj = getSQLDataType(dataType)
    StructField(name, dtObj, nullable)
  }

  def createStructField(name: String, struct: StructType): StructField = {
    StructField(name, struct, true)
  }

  def createStructFields(specs: Seq[Any]): Array[StructField] = {
    specs.map(
      x => {
        val spec = x.asInstanceOf[Array[Object]]

        createStructField(
          name = spec(0).asInstanceOf[String],
          dataType = spec(1).asInstanceOf[String],
          nullable = spec(2).asInstanceOf[Boolean]
        )
      }
    ).toArray
  }
}
