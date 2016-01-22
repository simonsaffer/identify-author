package com.identifyauthor.processing

import java.net.URI

import org.apache.hadoop.fs.{LocatedFileStatus, Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simonsaffer on 2016-01-22.
  */
object Processor {

  def countWords(rdd: RDD[String]) = {
    val counts = rdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println(_))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Identify Authors").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val uri: URI = new URI("hdfs://localhost:9000")

    val files = FileSystem.get(uri, sc.hadoopConfiguration).listFiles(new Path("/books"), true)

    while (files.hasNext) {

      val file = files.next()

      countWords(sc.textFile(file.getPath.toString))

    }

  }

}
