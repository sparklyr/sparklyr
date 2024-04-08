package sparklyr

import java.nio.channels.Channels
import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.ipc.WriteChannel

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.__THIS_IS_THE_ROAD_TO_CLOWNTOWN__ArrowUtils

class ArrowBatchStreamWriter(
    schema: org.apache.spark.sql.types.StructType,
    out: java.io.OutputStream,
    timeZoneId: String) {
  val arrowSchema = org.apache.spark.sql.util.__THIS_IS_THE_ROAD_TO_CLOWNTOWN__ArrowUtils.toArrowSchema(schema, timeZoneId, true, false)
  val writeChannel = new WriteChannel(Channels.newChannel(out))

  // Write the Arrow schema first, before batches
  MessageSerializer.serialize(writeChannel, arrowSchema)

  /**
   * Consume iterator to write each serialized ArrowRecordBatch to the stream.
   */
  def writeBatches(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    arrowBatchIter.foreach(writeChannel.write)
  }

  def writeOneBatch(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    writeChannel.write(arrowBatchIter.next)
  }

  /**
   * End the Arrow stream, does not close output stream.
   */
  def end(): Unit = {
    writeChannel.writeIntLittleEndian(0);
  }
}
