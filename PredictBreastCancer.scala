package com.kamali.exe
import org.apache.spark.mllib._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
object PredictBreastCancer{
  
  def main(agrs:Array[String]):Unit={
    
    
    val sc=new SparkContext("local[*]","BreastCancer")
    sc.setLogLevel("WARN")
    val RDD= sc.textFile("/home/kiran/breastCancer.csv")
    
    val data=RDD.map{line=>
      val values=line.split(",").map(_.toDouble)
      val featurevector= Vectors.dense(values.init)
      val label=values.last-1
      LabeledPoint(label,featurevector)
      }
   val Array(traindata,testData)=data.randomSplit(Array(0.9,0.11))
   traindata.cache
   
   val model=DecisionTree.trainClassifier(traindata,7,Map[Int,Int](), "gini",6,100)
   
  def getMetrics(model:DecisionTreeModel,data:RDD[LabeledPoint])={  

                val predictionsAndLabels = data.map(example => (model.predict(example.features), example.label))
  
                new MulticlassMetrics(predictionsAndLabels)
              }   
                
   
   val metrics = getMetrics(model, testData)
  
   println(metrics.confusionMatrix)
   println(metrics.precision)
   
  }
}

