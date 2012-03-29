package wiki

import spark.SparkContext
import SparkContext._

import java.io.StringReader

import org.apache.lucene.analysis.tokenattributes._
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer

import scala.collection.mutable.MutableList


object WikiClustering {

  def produceWordCounts(sc: SparkContext, input: String, output: String): Array[(String, Int)] = {
    val wikiFile = sc.textFile(input)
  
    val tokens = wikiFile flatMap { line => {
      val tf = new WikipediaTokenizer(new StringReader(line))
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

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "WikiClustering")
    val wordCounts = produceWordCounts(
      sc,
      "/scratch/rxin/wikipedia/xai",
      "/scratch/rxin/wikipedia/out/wordcounts")
  }
}

