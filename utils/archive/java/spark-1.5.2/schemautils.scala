package sparklyr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

object SchemaUtils {
  private[this] sealed trait DType {}
  private[this] case class Field(name: String, dtype: DType)
  private[this] case class GenericDType(repr: String) extends DType
  private[this] case class StructDType(fields: Array[Field]) extends DType
  private[this] case class NestedColsDType(elementType: DType) extends DType

  def sdfSchema(
    sdf: DataFrame,
    expandNestedCols: Boolean,
    expandStructCols: Boolean
  ): String = {
    implicit val formats = org.json4s.DefaultFormats

    val schema = structTypeToSchema(
      structType = sdf.schema,
      expandNestedCols = expandNestedCols,
      expandStructCols = expandStructCols
    )

    org.json4s.jackson.Serialization.write(schema)
  }

  private[this] def structTypeToSchema(
    structType: StructType,
    expandNestedCols: Boolean,
    expandStructCols: Boolean
  ): DType = {
    val fields = structType.fields.map(
      x => {
        Field(
          name = x.name,
          dtype = {
            val dtype = x.dataType
            if (expandStructCols && dtype.isInstanceOf[StructType]) {
              structTypeToSchema(
                structType = dtype.asInstanceOf[StructType],
                expandNestedCols = expandNestedCols,
                expandStructCols = expandStructCols
              )
            } else if (expandNestedCols && isStructArr(dtype)) {
              val elementType = dtype.asInstanceOf[ArrayType].elementType

              NestedColsDType(
                elementType = structTypeToSchema(
                  structType = elementType.asInstanceOf[StructType],
                  expandNestedCols = expandNestedCols,
                  expandStructCols = expandStructCols
                )
              )
            } else {
              GenericDType(repr = dtype.toString)
            }
          }
        )
      }
    )

    StructDType(fields = fields)
  }

  private[this] def isStructArr(dtype: DataType): Boolean = {
    if (dtype.isInstanceOf[ArrayType]) {
      val arrayType = dtype.asInstanceOf[ArrayType]

      arrayType.elementType.isInstanceOf[StructType]
    } else {
      false
    }
  }

  def castColumns(sdf: DataFrame, desiredSchema: StructType): DataFrame = {
    val desiredColumnTypes = extractColumnTypes(desiredSchema)
    val inputColumnTypes = extractColumnTypes(sdf.schema)

    val outputColumns = sdf.schema.fieldNames.map(
      column => {
        val columnType = desiredColumnTypes.getOrElse(
          column, inputColumnTypes.get(column).get
        )

        new org.apache.spark.sql.Column(column).cast(columnType)
      }
    )

    sdf.select(outputColumns: _*)
  }

  private[this] def extractColumnTypes(schema: StructType): Map[String, DataType] = {
    schema.fields.map(x => x.name -> x.dataType).toMap
  }
}
