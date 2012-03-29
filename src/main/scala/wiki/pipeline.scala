package wiki

import spark._
import SparkContext._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.JobConf

import org.apache.lucene.analysis.tokenattributes._
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer

import java.io.StringReader
import scala.collection.mutable.MutableList
import scala.util.matching.Regex


object WordCount {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "WikiClustering")
    val wikiRdd = Pipeline.createWikiRdd(sc, args(1))
    produceWordCounts(wikiRdd, args(2))
  }

  def produceWordCounts(wikiRdd: RDD[WikiDoc], output: String): Array[(String, Int)] = {
    val tokens = wikiRdd flatMap { doc => {
      val tf = new WikipediaTokenizer(new StringReader(doc.text))
      val termAtt: TermAttribute = tf.addAttribute(classOf[TermAttribute])
      val typeAtt: TypeAttribute = tf.addAttribute(classOf[TypeAttribute])

      val tokens = new MutableList[String]()
      while (tf.incrementToken()) { tokens += termAtt.term() }
      tokens
    }}

    val counts = tokens.map((_, 1))
      .reduceByKey(_ + _)
      .filter { case(w, c) => c > 2 }

    val countsSorted = counts.map { case(w, c) => (c, w) }
      .sortByKey(ascending=false)
      .map { case(c, w) => (w, c) }
    
    countsSorted.saveAsTextFile(output)
    countsSorted.collect()
  }
}


object Pipeline {

  def createWikiRdd(sc: SparkContext, path: String): RDD[WikiDoc] = {
    val conf = new JobConf
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")
    conf.set("io.file.buffer.size", System.getProperty("spark.buffer.size", "65536"))
  
    FileInputFormat.setInputPaths(conf, path)
    sc.hadoopRDD(conf, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => new WikiDoc(pair._2.toString))
  }
  
}

