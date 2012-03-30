package wiki

import spark._
import SparkContext._

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.JobConf

import org.apache.lucene.analysis._
import org.apache.lucene.analysis.tokenattributes._
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer
import org.apache.lucene.util.Version.LUCENE_35

import org.yagnus.yadoop.IntArrayWritable
import org.yagnus.yadoop.Yadoop.intSeq2IntArrayWritable

import java.io.{File, FileWriter, StringReader}
import scala.collection.immutable.HashMap
import scala.collection.mutable.MutableList
import scala.util.matching.Regex


object Pipeline {

  def main(args: Array[String]) {
    val inputPath = args(1)
    val baseOutputPath = args(2)

    (new File(baseOutputPath + "/default")).mkdirs()
    (new File(baseOutputPath + "/stem")).mkdirs()
    (new File(baseOutputPath + "/stop")).mkdirs()
    (new File(baseOutputPath + "/stemStop")).mkdirs()

    val sc = new SparkContext(args(0), "WikiClustering")
    val wikiRdd = Pipeline.createWikiRdd(sc, inputPath)
    runPipeline(wikiRdd, Pipeline.defaultTok, baseOutputPath + "/default")
    runPipeline(wikiRdd, Pipeline.stemTok, baseOutputPath + "/stem")
    runPipeline(wikiRdd, Pipeline.stopTok, baseOutputPath + "/stop")
    runPipeline(wikiRdd, Pipeline.stemStopTok, baseOutputPath + "/stemStop")
  }

  def runPipeline(wikiRdd: RDD[WikiDoc], tok: (String => TokenStream), outpath: String) {
    // Do the word count and generate the dictionary.
    val wordCounts = produceWordCounts(wikiRdd, tok, outpath + "/dict.txt")
    val wordIndexes = wordCounts.zipWithIndex.map { case(wc, i) => (wc._1, i) }

    // Build a hash map for word counts.
    val dict = new HashMap[String, Int]() ++ wordIndexes

    generateFeatureFiles(wikiRdd, dict, tok, outpath + "/doc-ints")
  }

  def generateFeatureFiles(
    wikiRdd: RDD[WikiDoc],
    dict: Map[String, Int],
    tok: (String => TokenStream),
    outpath: String) {
    
    // Tokenized doc.
    val tokenizedDocs: RDD[(Int, Seq[Int])] = wikiRdd.map { doc =>
      (doc.id, tokenizeText(doc.text, tok).filter(dict.contains(_)).map(dict(_)))
    }
    tokenizedDocs.saveAsSequenceFile(outpath)
  }

  def produceWordCounts(wikiRdd: RDD[WikiDoc], tok: (String => TokenStream), outpath: String)
  : Array[(String, Int)] = {
    // Flat map all documents into tokens (words).
    val tokens = wikiRdd flatMap { doc => tokenizeText(doc.text, tok) }

    // Count the words. Filter out the most uncommon ones.
    val counts = tokens.map((_, 1))
      .reduceByKey(_ + _)
      .filter { case(w, c) => c > 2 }

    // Sort the words in descending order.
    val countsSorted = counts.map { case(w, c) => (c, w) }
      .sortByKey(ascending=false)
      .map { case(c, w) => (w, c) }

    //countsSorted.map { case(w, c) => w + "\t" + c }.saveAsTextFile(outpath)
    val countsSortedArray = countsSorted.collect()
    Pipeline.writeArrayOfPairToDisk(countsSortedArray, outpath, true)
    //val out = new FileWriter(outpath)
    //countsSortedArray.foreach { case(c, w) => out.write(w + "\t" + c + "\n") }
    //out.close()

    countsSortedArray
  }

  def tokenizeText(text: String, tok: (String => TokenStream)): Seq[String] = {
    val tokenStream = tok(text)
    val termAtt: TermAttribute = tokenStream.addAttribute(classOf[TermAttribute])
    val typeAtt: TypeAttribute = tokenStream.addAttribute(classOf[TypeAttribute])
    val tokens = new MutableList[String]()
    while (tokenStream.incrementToken()) { tokens += termAtt.term() }
    tokenStream.end()
    tokenStream.close()
    tokens
  }

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

  def writeArrayOfPairToDisk[K, V](
    arr: Array[(K, V)], path: String, addLineNumber: Boolean = false) {
    val out = new FileWriter(path)
    arr.zipWithIndex.foreach { case(pair, lineNumber) => {
      if (addLineNumber) {
        out.write(lineNumber + "\t")
      }
      out.write(pair._1 + "\t" + pair._2 + "\n")
    }}
    out.close()
  }
   
}

