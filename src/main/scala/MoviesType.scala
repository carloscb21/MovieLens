import org.apache.spark.{SparkContext, SparkConf}


object MoviesType {

  def main(args: Array[String]): Unit = {
    val logFile = "resources//movies.csv"
    val conf = new SparkConf().setAppName("MoviesType").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(logFile)
    val genre_moviesRDD = textFile.map(x =>{

      val movie = x.split(",")
      var movie_length = movie.length

      val film = movie(1)
      val genres = movie(movie_length-1).split('|')
      (genres, film)

    })

    val movies_mapperRDD = genre_moviesRDD.flatMap{case(genres, movie) => genres.map(genre => (genre,movie))}
    val movies_reducerRDD = movies_mapperRDD.groupBy(x=> x._1)

    //movies_mapperRDD.take(1000).foreach(movie => println(s"${movie._1}, ${movie._2}"))
    movies_reducerRDD.take(3).foreach(println)
  }

}
