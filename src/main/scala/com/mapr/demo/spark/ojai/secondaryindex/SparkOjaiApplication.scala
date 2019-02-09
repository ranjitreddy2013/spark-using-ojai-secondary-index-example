/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.demo.spark.ojai.secondaryindex

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.mapr.db.spark.impl.OJAIDocument
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.ojai.types.ODate
import com.mapr.db.spark.{field, _}
import org.ojai.store.DriverManager
import scala.collection.mutable.ListBuffer

object SparkOjaiApplication {

  val userInfo = "/tmp/user-info"
  val dataTable = "/tmp/data-table"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark-OJAI Secondary Index Application").master("local[*]").getOrCreate()
    val allUsers = spark.sparkContext.parallelize(getUsers())
    val sc = spark.sparkContext

    //Save users to JSON table
    allUsers.saveToMapRDB(userInfo, createTable = false
    )

    //Load all the people from the JSON table
    val allUsersInfo = sc.loadFromMapRDB(userInfo)

    //Extract JSON documents using secondary index
    val documentsRDD = allUsersInfo.mapPartitions(getDocuments)

    // print a few documents
    documentsRDD.take(3).foreach(println(_))

    System.out.println("Number of documents extracted:" + documentsRDD.count())
  }

  //Invokes OJAI api to query JSON documents using seconary index.
  def getDocuments(iterator: Iterator[OJAIDocument]): Iterator[String] = {
    val connection = DriverManager.getConnection("ojai:mapr:")
    val store = connection.getStore(dataTable)
    val dm  = ListBuffer[String]()

    iterator.foreach(r => {
      val qs = "{\"$eq\": {\"uid\":\"%s\"}}".format(r.getDoc.getId.getString)
      System.out.println("Finding  documents for qs:" + qs);
      val  query = connection.newQuery().select("_id")
        //This option is not required. OJAI client makes the determination to use secondary index.
        // Since the sample data set is small, I'm enabling this option to use secondary index. 
        .setOption(com.mapr.ojai.store.impl.OjaiOptions.OPTION_USE_INDEX, "uid_idx")
        .where(qs).build()
      val iterator = store.find(query).iterator()
      if (iterator.hasNext) {
        dm += iterator.next().asJsonString()
      }
    })

    //Close the Document Store
    store.close()

    //Close the OJAI connection
    connection.close()

    dm.toIterator
  }

  // User documents. The _id field of users is used to query  the user in the data-table.
  def getUsers(): Array[Person] = {
    val users: Array[Person] = Array(
      Person("101", ODate.parse("1976-1-9"), Seq("cricket", "sketching"), Map("city" -> "san jose", "street" -> "305 city way", "Pin" -> 95652)),
      Person("102", ODate.parse("1987-5-4"), Seq("squash", "comics", "movies"), Map("city" -> "sunnyvale", "street" -> "35 town way", "Pin" -> 95985))
    )
    users
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Person (@JsonProperty("_id") id: String, @JsonProperty("dob") dob: ODate,
                   @JsonProperty("interests") interests: Seq[String], @JsonProperty("address") address: Map[String, Any])
