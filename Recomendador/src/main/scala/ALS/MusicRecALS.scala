package als

import java.io.File

import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object MusicRecALS {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length != 1) {
      println("A value must be included")
      sys.exit(1)
    }

    // set up environment

    val conf = new SparkConf()
      .setAppName("MusicRecALS")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    
    var files="hdfs://quickstart.cloudera/user/MusicRecommender/model/part*"
    var events="hdfs://quickstart.cloudera/user/MusicRecommender/events/part*"

    val ratings = sc.textFile(files)
    .map(_.split("\t"))
    // format: (userId, artId, rating)
    .map(fields => (fields(0).toInt,fields(1),fields(2)))

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.userId).distinct().count()
    val numArtists = ratings.map(_._2.artId).distinct().count()
    
    val musicEvents = sc.textFile(events)
    .map(_.split("\t"))
    // format: (userId, artId)
    .map(fields => (fields(0).toInt,fields(1))
    .collect().toMap

    val myRatingsRDD = sc.parallelize(ratings, 1)

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numArtists + " artist.")

    // split ratings into train (60%), validation (20%), and test (20%) based on the 
    // last digit of the timestamp, add myRatings to train, and cache them

    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // train models and evaluate them on the validation set

    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = " 
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // evaluate the best model on the test set

    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model

    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = 
      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // make personalized recommendations

    val myRatedArtistsIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(musicEvents.keys.filter(!myRatedArtistsIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(- _.rating)
      //.take(50)
      .take(1)

    var i = 1
    println("Artist recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + musicEvents(r.artId))
      i += 1
    }

    // clean up
    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

}
