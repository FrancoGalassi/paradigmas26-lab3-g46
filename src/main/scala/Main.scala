import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext


    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    // lee archivo de subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile).flatten

    // carga en el rdd 
    val subs_rdd = sc.parallelize(subscriptionOpts)

    // Filter out malformed subscriptions (None values) 
    // val subscriptions = subs_rdd.flatten
    // ya estaba en subscriptionsOpts

    // Download feeds and parse posts, tracking success/failure
    // devuelve lista de todos los posts
    val downloadResults = subs_rdd.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      posts
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.count()
    /////val feedsFailed = downloadResults.length - feedsSuccess

    // Flatten all posts and count JSON parse failures
    // val allPosts = downloadResults.flatMap(_._2)  no hace falta porque ahora downloadResults no devuelve (bool, List[post])
    val postsSuccess = downloadResults.count()  // cuenta la cantidad de posts en la lista
    /////val postsFailed = downloadResults.count(_._2.isEmpty)

    // Filter empty posts 
    // el del analyzer pero adaptado para rdd
    val filteredPosts = downloadResults.filter { post =>
        post.title.nonEmpty &&
        post.selftext.nonEmpty &&
        post.selftext.trim.nonEmpty
      }

    val postsFiltered = downloadResults.count() - filteredPosts.count()

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.count() > 0) totalChars / filteredPosts.count() else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> 0,   // 0 hasta arreglarlo
      "feedsFailed" -> 0,
      "postsSuccess" -> postsSuccess.toInt, 
      "postsFailed" -> 0,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt //.toint porque count devuelve long y stats espera Map[String, Int]
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.count() == 0) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)


    //  es del ej 3 arreglar esto
    //  Detect entities in all posts (combine title and selftext)
      val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    //como el analyzer usa listas, se guarda en spark con collect().toList

    val allEntitiesList = allEntities.collect().toList
    // Count entities
    val entityCounts = Analyzer.countEntities(allEntitiesList)
    val typeStats = Analyzer.countByType(allEntitiesList)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
