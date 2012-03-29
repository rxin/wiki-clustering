
name := "wiki"

version := "1.0"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analyzers" % "3.5.0" % "compile->default"
)

resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo"
)

