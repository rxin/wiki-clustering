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
    val wikiRdd = Pipeline.createWikiRddFromXml(sc, inputPath)
    generateEncodedDocs(wikiRdd, Pipeline.defaultTok, baseOutputPath + "/default")
    convertEncodedDocs2BagOfWords(
      sc,
      baseOutputPath + "/default/docs-encoded",
      baseOutputPath + "/default/docs-bagsOfWords")

    generateEncodedDocs(wikiRdd, Pipeline.stemTok, baseOutputPath + "/stem")
    convertEncodedDocs2BagOfWords(
      sc,
      baseOutputPath + "/stem/docs-encoded",
      baseOutputPath + "/stem/docs-bagsOfWords")

    generateEncodedDocs(wikiRdd, Pipeline.stopTok, baseOutputPath + "/stop")
    convertEncodedDocs2BagOfWords(
      sc,
      baseOutputPath + "/stop/docs-encoded",
      baseOutputPath + "/stop/docs-bagsOfWords")

    generateEncodedDocs(wikiRdd, Pipeline.stemStopTok, baseOutputPath + "/stemStop")
    convertEncodedDocs2BagOfWords(
      sc,
      baseOutputPath + "/stemStop/docs-encoded",
      baseOutputPath + "/stemStop/docs-bagsOfWords")
  }

  def generateEncodedDocs(wikiRdd: RDD[WikiDoc], tok: (String => TokenStream), outpath: String) {
    // Do the word count and generate the dictionary.
    val (encodingDict, dfDict) = produceDocumentFrequencyDict(wikiRdd, tok, outpath)
    encodeDocuments(wikiRdd, encodingDict, tok, outpath)
  }

  def convertEncodedDocs2BagOfWords(sc: SparkContext, in: String, out: String) {
    val docs: RDD[(IntWritable, IntArrayWritable)] = sc.sequenceFile(in)
    val bagOfWordsDocs: RDD[(IntWritable, Seq[Int])] = docs.map { case(id, tokens) =>
      val wordCountsMap: Map[Int, Int] = tokens.get.groupBy(x => x).mapValues(_.length)
      (id, wordCountsMap.flatMap(x => x.productIterator).asInstanceOf[Iterable[Int]].toSeq)
    }
    bagOfWordsDocs.saveAsSequenceFile(out)
  }

  /**
   * Use the supplied encoding dictionary to encode documents (replace string
   * tokens with Ints) and save it in a sequence file. The sequence file is of
   * type: (IntWritable, IntArrayWritable).
   */
  def encodeDocuments(
    wikiRdd: RDD[WikiDoc],
    encodingDict: Map[String, Int],
    tok: (String => TokenStream),
    outpath: String) {
    
    // Tokenized doc.
    val tokenizedDocs: RDD[(Int, Seq[Int])] = wikiRdd.map { doc =>
      (doc.id, tokenizeText(doc.text, tok).filter(encodingDict.contains(_)).map(encodingDict(_)))
    }
    tokenizedDocs.saveAsSequenceFile(outpath + "/docs-encoded")
  }

  /**
   * Generate the word dictionary for encoding and document frequencies. It
   * should be small enough to store as a hash map on all nodes.
   */
  def produceDocumentFrequencyDict(
    wikiRdd: RDD[WikiDoc],
    tok: (String => TokenStream),
    outpath: String)
  : (Map[String, Int], Map[Int, Int]) = {

    val dfSorted: Array[(String, Int)] = wikiRdd.flatMap(d => tokenizeText(d.text, tok).distinct)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter{case(w, c) => c > 50}
      .collect()
      .sortBy(_._2)
      .reverse

    writeArrayOfPairToDisk(dfSorted, outpath + "/dict-df.txt", true)

    // encodingDict maps a word to its integer id.
    val encodingDict = dfSorted.zipWithIndex.map { case(wc, i) => (wc._1, i) }.toMap
    // dfDict maps an integer id to the document frequency of the word.
    val dfDict = dfSorted.zipWithIndex.map { case(wc, i) => (i, wc._2) }.toMap
    (encodingDict, dfDict)
  }

  /**
   * Given some text in string, tokenize it and return a sequence of tokens.
   */
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

  /**
   * Create RDD from the Wikipedia XML dump.
   */
  def createWikiRddFromXml(sc: SparkContext, path: String): RDD[WikiDoc] = {
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

