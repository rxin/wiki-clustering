package wiki

import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntLinkedOpenHashMap}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ReflectionUtils

import org.yagnus.yadoop.IntArrayWritable
import org.yagnus.yadoop.Yadoop.intSeq2IntArrayWritable

import java.net.URI


object Clustering {

  def main(args: Array[String]) {
    val features: Map[Int, Int2IntMap] = buildFeatureMatrix(args(0))
    
    println(features.size)
  }

  /**
   * Build the bag-of-words feature matrix from input files in the form of an
   * immutable Map. For each entry, the map key is the integer id, and map value
   * is an Int2IntMap that maps the index of each token to the number of times
   * the token appears in the corresponding document.
   */
  def buildFeatureMatrix(inputPath: String): Map[Int, Int2IntMap] = {
    val conf = new JobConf
    val path = new Path(inputPath)
    val fs = FileSystem.get(URI.create(inputPath), conf)
    val files = fs.listStatus(path)

    val progress = new java.util.concurrent.atomic.AtomicInteger(0)
    println("reading " + files.length + " files:")

    // Parallelize to 24 threads on watson.
    val allDocs = files.zipWithIndex.par.map { case(status, i) => {
      println("reading file #" + progress.incrementAndGet() + ": " + status.getPath) 

      val docs = scala.collection.mutable.ArrayBuffer[(Int, Int2IntMap)]()

      val reader = new Reader(fs, status.getPath, conf)
      val key = ReflectionUtils.newInstance(reader.getKeyClass(), conf).asInstanceOf[IntWritable]
      val value = ReflectionUtils.newInstance(
        reader.getValueClass(), conf).asInstanceOf[IntArrayWritable]

      while (reader.next(key, value)) {
        val k: Int = key.get
        val v: Seq[Int] = value.get

        val wordCountsMap = new Int2IntLinkedOpenHashMap()
        (0 to (v.length / 2 - 1)) foreach { i =>
          wordCountsMap.add(v(i * 2), v(i * 2 + 1))
        }

        docs += Pair(k, wordCountsMap)
      }
      reader.close()
      docs
    }}

    collection.immutable.Map() ++ allDocs.flatten
  }
}

