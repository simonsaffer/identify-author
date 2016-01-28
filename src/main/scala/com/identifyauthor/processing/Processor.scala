package com.identifyauthor.processing

import java.net.URI

import org.apache.hadoop.fs.{LocatedFileStatus, Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simonsaffer on 2016-01-22.
  */
object Processor {

  def countWords(words: RDD[String]) = {
    val counts = words.map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def calcNumberOfDistinctWordsUsed(words: RDD[String]) = words.distinct().count()

  // Does the author prefer some subset of all the words used
  def wordProbabilityDistribution(words: RDD[String]) = 1

  def avgWordLength(words: RDD[String], numberOfDistinctWords: Long) = {
    words.map(_.length).reduce(_+_) / numberOfDistinctWords
  }

  def stopWordRatio(words: RDD[String], numberOfDistinctWords: Long, stopWords: RDD[String]) = words.union(stopWords)

  def preprocessFile(rawRDDFromFile: RDD[String]): RDD[String] = {
    rawRDDFromFile
      .map(_.replaceAll("[^A-Za-z ]", ""))
      .map(_.toLowerCase())
      .flatMap(_.split(" "))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Identify Authors").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val uri: URI = new URI("hdfs://localhost:9000")

    val files = FileSystem.get(uri, sc.hadoopConfiguration).listFiles(new Path("/books"), true)

    val stopWords = sc.textFile("stopwords.txt")

    while (files.hasNext) {

      val filePath: String = files.next().getPath.toString
      val words: RDD[String] = preprocessFile(sc.textFile(filePath))

      val numberOfDistinctWordsUsed = calcNumberOfDistinctWordsUsed(words)

      val averageWordCount = avgWordLength(words, numberOfDistinctWordsUsed)

      val stopWordsRatio = stopWordRatio(words, numberOfDistinctWordsUsed, stopWords)

      countWords(words)

    }


  }

}
