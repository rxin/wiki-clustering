package wiki

import scala.collection.mutable.MutableList
import scala.util.matching.Regex


class WikiDoc (doc: String) {
  lazy val id: Int = WikiDoc.extractDocId(doc)
  lazy val title: String = WikiDoc.extractTitle(doc)
  lazy val timestamp: String = WikiDoc.extractTimestamp(doc)
  lazy val author: String = WikiDoc.extractAuthor(doc)
  val text: String = doc
  lazy val categories: Seq[String] = WikiDoc.extractCategories(doc)
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

