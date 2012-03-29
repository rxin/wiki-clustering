package wiki

import scala.collection.mutable.MutableList
import scala.util.matching.Regex


class WikiDoc (
  val id: Int,
  val title: String,
  val timestamp: String,
  val author: String,
  val text: String,
  val categories: Seq[String]) {

  def this(doc: String) = {
    // TODO(rxin): extract content between
    // <text xml:space="preserve">
    // and
    // </text>
    // for text.
    this(
      WikiDoc.extractDocId(doc),
      WikiDoc.extractTitle(doc),
      WikiDoc.extractTimestamp(doc),
      WikiDoc.extractAuthor(doc),
      doc,
      WikiDoc.extractCategories(doc))
  }  
}


object WikiDoc {
  
  val CATEGORY_REGEX = "\\[\\[Category\\:(.+?)\\]\\]".r
  val TITLE_REGEX = "<title>(.+?)</title>".r
  val DOC_ID_REGEX = "<id>(\\d+)</id>".r
  val AUTHOR_REGEX = "<username>(.+?)</username>".r
  val TIMESTAMP_REGEX = "<timestamp>(.+?)</timestamp>".r

  def extractCategories(doc: String): Seq[String] = {
    val matches = new MutableList[String]()
    val matchesIter = CATEGORY_REGEX.findAllIn(doc)
    while (matchesIter.hasNext) {
      matches += matchesIter.group(1)
      matchesIter.next()
    }
    matches
  }

  def extractTitle(doc: String): String = extractSingleMatch(doc, TITLE_REGEX)

  def extractAuthor(doc: String): String = extractSingleMatch(doc, AUTHOR_REGEX)

  def extractDocId(doc: String): Int = extractSingleMatch(doc, DOC_ID_REGEX).toInt

  def extractTimestamp(doc: String): String = extractSingleMatch(doc, TIMESTAMP_REGEX)

  /**
   * A helper function to extract a single match in a document. This works for
   * document title, ID, and author.
   */
  def extractSingleMatch(doc: String, regex: Regex): String = {
    regex.findFirstMatchIn(doc) match {
      case Some(m) => m.group(1)
      case None => ""
    }
  }
}

