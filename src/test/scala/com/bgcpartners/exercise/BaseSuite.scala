package com.bgcpartners.exercise

import java.io.File
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

/**
  * Created by adnan_saqib on 10/11/2019.
  */
abstract class BaseSuite extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  @transient protected var _spark: SparkSession = _
  protected def spark: SparkSession = _spark

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.sql.shuffle.partitions","2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setSparkHome(System.getenv("SPARK_HOME"))
    _spark = SparkSession.builder().config(conf).getOrCreate
  }

  protected def assertRDDEquals[T: ClassTag](expectedRDD: RDD[T], resultRDD: RDD[T])(checkEquals: (T, T) => Boolean) =
    try {
      expectedRDD.cache
      resultRDD.cache
      assert(expectedRDD.count === resultRDD.count)

      val expectedIndexValue = zipWithIndex(expectedRDD)
      val resultIndexValue = zipWithIndex(resultRDD)

      val unequalRDD = expectedIndexValue.join(resultIndexValue)
        .filter {
          case (idx, (t1: T, t2: T)) => !checkEquals(t1, t2)
        }
      unequalRDD.collect().foreach(println)
      assert(unequalRDD.take(10).length === 0)
    } finally {
      expectedRDD.unpersist()
      resultRDD.unpersist()
    }

  private def zipWithIndex[U: ClassTag](rdd: RDD[U]) =
    rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

  protected def createDF(file: String): DataFrame =
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", "\t")
      .csv(getResourcePath(file))

  protected def assertDataFrameEquals(expectedDF: DataFrame, resultDF: DataFrame) =
    assertRDDEquals(expectedDF.rdd, resultDF.rdd) {
      //Comparing rows from the actual and expected data frames
      (row1: Row, row2: Row) => {
        val isRowEqual : Boolean = if(row1.length != row2.length) {
          false
        } else {
          val areAllRowElementsEqual = (0 until row1.length).forall(idx => {
            val isCellEqual : Boolean = if(!row1.isNullAt(idx) && !row2.isNullAt(idx)) {
              val o1 = row1.get(idx)
              val o2 = row2.get(idx)
              val isCellDataEqual :Boolean = o1 match {
                case d1: Double =>
                  if ((java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(o2.asInstanceOf[Double]))
                    && d1 - o2.asInstanceOf[Double] != 0.0) {
                    false
                  } else true
                case s1: String =>
                  s1.equals(o2.asInstanceOf[String])
                case _ =>
                  if (o1 != o2) false else true
              }
              isCellDataEqual
            } else false
            isCellEqual
          }
          )
          areAllRowElementsEqual
        }
        isRowEqual
      }
    }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  private final def getResourceFile(file: String): File =
    new File(getClass.getClassLoader.getResource(file).getFile)

  protected final def getResourcePath(file: String): String =
    getResourceFile(file).getCanonicalPath

  override def afterAll() {
    _spark = null
    super.afterAll()
  }
}
