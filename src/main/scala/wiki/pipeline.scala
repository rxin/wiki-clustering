package wiki

import spark._
import SparkContext._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.JobConf

import org.apache.lucene.analysis._
import org.apache.lucene.analysis.tokenattributes._
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer
import org.apache.lucene.util.Version.LUCENE_35

import java.io.{File, FileWriter, StringReader}
import scala.collection.mutable.MutableList
import scala.util.matching.Regex


object WordCount {

  def main(args: Array[String]) {
    val inputPath = args(1)
    val baseOutputPath = args(2)
    val sc = new SparkContext(args(0), "WikiClustering")
    val wikiRdd = Pipeline.createWikiRdd(sc, inputPath)
    wikiRdd.cache()
    produceWordCounts(wikiRdd, baseOutputPath + "/default", Pipeline.defaultTok)
    produceWordCounts(wikiRdd, baseOutputPath + "/stem", Pipeline.stemTok)
    produceWordCounts(wikiRdd, baseOutputPath + "/stop", Pipeline.stopTok)
    produceWordCounts(wikiRdd, baseOutputPath + "/stemStop", Pipeline.stemStopTok)
  }

  def produceWordCounts(wikiRdd: RDD[WikiDoc], outpath: String, tok: (String => TokenStream))
  : Array[(String, Int)] = {
    // Flat map all documents into tokens (words).
    val tokens = wikiRdd flatMap { doc => {
      val tokenStream = tok(doc.text)
      val termAtt: TermAttribute = tokenStream.addAttribute(classOf[TermAttribute])
      val typeAtt: TypeAttribute = tokenStream.addAttribute(classOf[TypeAttribute])

      val tokens = new MutableList[String]()
      while (tokenStream.incrementToken()) { tokens += termAtt.term() }
      tokenStream.end()
      tokenStream.close()
      tokens
    }}

    // Count the words. Filter out the most uncommon ones.
    val counts = tokens.map((_, 1))
      .reduceByKey(_ + _)
      .filter { case(w, c) => c > 2 }

    // Sort the words in descending order.
    val countsSorted = counts.map { case(w, c) => (c, w) }
      .sortByKey(ascending=false)
      .map { case(c, w) => (w, c) }
    
    countsSorted.map { case(w, c) => w + "\t" + c }.saveAsTextFile(outpath)
    val countsSortedArray = countsSorted.collect()

    //val out = new FileWriter(outpath)
    //countsSortedArray.foreach { case(c, w) => out.write(w + "\t" + c + "\n") }
    //out.close()

    countsSortedArray
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
 
  def defaultTok(text: String): TokenStream = {
    new LowerCaseFilter(LUCENE_35, new WikipediaTokenizer(new StringReader(text)))
  }

  def stemTok(text: String): TokenStream = {
    new PorterStemFilter(defaultTok(text))
  }

  def stopTok(text: String): TokenStream = {
    new StopFilter(LUCENE_35, defaultTok(text), StopAnalyzer.ENGLISH_STOP_WORDS_SET)
  }

  def stemStopTok(text: String): TokenStream = {
    new StopFilter(LUCENE_35, stemTok(text), StopAnalyzer.ENGLISH_STOP_WORDS_SET)
  }
   
}

