package sparklyr

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Iterator interface to iterate over Arrow record batches and return rows
 */
trait ArrowRowIterator extends Iterator[Row] {

  /**
   * Return the schema loaded from the Arrow record batch being iterated over
   */
  def schema: StructType
}

object ArrowConverters {

  /**
   * Maps Iterator from ArrowPayload to Row. Returns a pair containing the row iterator
   * and the schema from the first batch of Arrow data read.
   */
  def fromPayloadIterator(
      payloadIter: Iterator[Array[Byte]],
      context: TaskContext,
      schema: StructType): ArrowRowIterator = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromPayloadIterator", 0, Long.MaxValue)

    new ArrowRowIterator {
      private var reader: ArrowFileReader = null
      private var rowIter = if (payloadIter.hasNext) nextBatch() else Iterator.empty

      context.addTaskCompletionListener { _ =>
        closeReader()
        allocator.close()
      }

      override def hasNext: Boolean = rowIter.hasNext || {
        closeReader()
        if (payloadIter.hasNext) {
          rowIter = nextBatch()
          true
        } else {
          allocator.close()
          false
        }
      }

      override def next(): Row = rowIter.next()

      private def closeReader(): Unit = {
        if (reader != null) {
          reader.close()
          reader = null
        }
      }

      private val encoder = RowEncoder(schema)

      private def nextBatch(): Iterator[Row] = {
        val in = new ByteArrayReadableSeekableByteChannel(payloadIter.next())
        reader = new ArrowFileReader(in, allocator)
        reader.loadNextBatch()  // throws IOException
        val root = reader.getVectorSchemaRoot  // throws IOException

        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(columns)
        batch.setNumRows(root.getRowCount)
        batch.rowIterator().asScala.map(x => Row(x.toSeq(schema)))
      }
    }
  }

  def toDataFrame(
      payloadRDD: JavaRDD[Array[Byte]],
      schema: StructType,
      sparkSession: SparkSession): DataFrame = {
    val rdd = payloadRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromPayloadIterator(iter, context, schema)
    }
    sparkSession.createDataFrame(rdd, schema)
  }
}
