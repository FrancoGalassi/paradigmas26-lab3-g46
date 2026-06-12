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
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)
      match {
        case Right(subs) => subs
        case Left(error) => 
          println(error)
          return
      }
    // si no hay suscripciones validas, salir con error
    if (subscriptionOpts.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    val feedsSuccess = sc.longAccumulator("feedsSuccess")
    val feedsFailed = sc.longAccumulator("feedsFailed")
    val postsSuccess = sc.longAccumulator("postsSuccess")
    val postsFailed = sc.longAccumulator("postsFailed")

    // carga en el rdd 
    val subs_rdd = sc.parallelize(subscriptionOpts)

    // Filter out malformed subscriptions (None values) 
    // val subscriptions = subs_rdd.flatten
    // ya estaba en subscriptionsOpts

    // Download feeds and parse posts, tracking success/failure
    // devuelve lista de todos los posts
    val downloadResults = subs_rdd.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      feedOpt match {
        case None => {
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          feedsFailed.add(1) // incrementa contador de feeds fallidos
          List()
        }  
        case Some(content) => {
          feedsSuccess.add(1)
          val postsOpt = JsonParser.parsePosts(content, subscription.name)
          // JsonParser.parsePosts ahora devuelve Option[List[Post]].
          postsOpt match {
            case None =>
              println(s"Warning: Failed to parse JSON from '${subscription.name}'")
              postsFailed.add(1)
              List()
            case Some(posts) =>
              postsSuccess.add(posts.length.toLong)
              posts
          }
        }
      }
    }
    // Filter empty posts 
    // el del analyzer pero adaptado para rdd
    val filteredPosts = downloadResults.filter { post =>
        post.title.nonEmpty &&
        post.selftext.nonEmpty &&
        post.selftext.trim.nonEmpty
    }

    val totalPosts = filteredPosts.count()

    val postsFiltered = postsSuccess.value - totalPosts

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (totalPosts > 0) totalChars / totalPosts else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.value.toInt,
      "feedsFailed" -> feedsFailed.value.toInt,
      "postsSuccess" -> postsSuccess.value.toInt, 
      "postsFailed" -> postsFailed.value.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt //.toint porque count devuelve long y stats espera Map[String, Int]
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (totalPosts == 0) {
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

    // después de detectar las entidades, mapea a [(tipo, nombre), 1] para después contarlas 
    val entity_par = allEntities.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // Count entities
    val entityCounts = entity_par.reduceByKey((suma,valor) => suma + valor)

    // convierte las entities a map
    val entityCountsMap: Map[(String, String), Int] = entityCounts
      .collect()
      .toMap


    val typeStatsMap: Map[String, Int] = entityCounts
      .map { case ((tipo, _), count) => (tipo, count) }   // deja solo el tipo de cada entidad
      .reduceByKey((suma,valor) => suma + valor)          // combina su cantidad de apariciones
      .collect()
      .toMap + ("total" -> entityCountsMap.values.sum)

    println(Formatters.formatTypeStats(typeStatsMap))
    println()
    println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))
  }
}
