package sparklyr

import java.io.BufferedOutputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.nio.MappedByteBuffer

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.{DataFrame, SparkSession}

object RDSCollector {
  def collect(df: DataFrame, outputFiles: Seq[String], spark: SparkSession): Unit = {
    // create a binary file(s) on HDFS if we are connected to a Spark cluster,
    // or on local file system if Spark is running in local mode
    val numPartitions = df.rdd.getNumPartitions
    if (outputFiles.length != numPartitions) {
      throw new IllegalArgumentException(
        "Number of output file(s) does not match input dataframe's number of " +
        "partitions (" + outputFiles.length + " vs. " +  numPartitions + ")."
      )
    }

    val (transformedDf, dtypes) =
      DFCollectionUtils.prepareDataFrameForCollection(df)
    transformedDf.cache

    val partitionSizes = new Array[Int](numPartitions)
    PartitionUtils.computePartitionSizes(transformedDf).foreach(
      p => {
        val partitionId = p(0)
        val partitionSize = p(1)

        partitionSizes(partitionId) = partitionSize
      }
    )

    writeHeaders(spark.sparkContext, outputFiles, dtypes)

    val cols = transformedDf.columns

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedHadoopConf = new SerializableWritable(hadoopConf)

    (0 until cols.length).map(
      idx => {
        val colDf = transformedDf.select(cols(idx))
        colDf.cache

        colDf.foreachPartition(
          (iter: Iterator[Row]) => {
            val fs = FileSystem.get(serializedHadoopConf.value)
            val fos = fs.create(
              new Path(outputFiles(TaskContext.getPartitionId) + ".col_" + idx)
            )
            val bos = new BufferedOutputStream(fos)
            val dos = new DataOutputStream(bos)

            try {
              RUtils.writeColumn(
                dos,
                partitionSizes(TaskContext.getPartitionId),
                iter,
                dtypes(idx)._2
              )
            } finally {
              dos.close
              bos.close
              fos.close
            }
          }
        )
      }
    )

    val logger = new Logger("RDSCollector", 0)
    val fs = FileSystem.get(hadoopConf)
    outputFiles.foreach(
      o => {
        val inputs = Array[String](
          o + ".hdr"
        ) ++
          (0 until cols.length).map(
            idx => o + ".col_" + idx
          )
        val paths = inputs.map(x => new Path(x))

        fs match {
          case _: LocalFileSystem => {
            val localPaths = inputs.map(
              x => java.nio.file.Paths.get(new java.net.URI(x).getPath)
            )
            val ch = Files.newByteChannel(
              java.nio.file.Paths.get(new java.net.URI(o).getPath),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING
            )
            try {
              localPaths.foreach(
                path => {
                  val pch = FileChannel.open(path, StandardOpenOption.READ)
                  try {
                    val buf = pch.map(FileChannel.MapMode.READ_ONLY, 0, pch.size)
                    ch.write(buf)
                  } finally {
                    pch.close
                  }
                }
              )
            } finally {
              ch.close
            }
          }
          case _ => {
            val dst = new Path(o)
            fs.createNewFile(dst)
            fs.concat(dst, paths)
          }
        }

        paths.foreach(
          path => {
            try {
              fs.delete(path, false)
            } catch {
              case e: Throwable => {
                logger.logWarning(
                  "Failed to delete temporary file " + path + ": " + e.toString
                )
              }
            }
          }
        )
      }
    )
  }

  private[this] def writeHeaders(
    sc: SparkContext,
    outputFiles: Seq[String],
    dtypes: Seq[(String, String)]
  ): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    outputFiles
      .map(p => fs.create(new Path(p + ".hdr")))
      .foreach(
        fos => {
          val bos = new BufferedOutputStream(fos)
          val dos = new DataOutputStream(bos)
          try {
            RUtils.writeXdrHeader(dos)
            RUtils.writeDataFrameHeader(dos, dtypes)
          } finally {
            dos.close
            bos.close
            fos.close
          }
        }
      )
  }
}
