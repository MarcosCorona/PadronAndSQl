import org.apache.spark.sql.SparkSession

object ExercisesSql extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Padron.07")
    .getOrCreate();

  val jdbcDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/sakila")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "actors")
    .option("user", "[root]")
    .option("password", "[x]")
    .load()

  jdbcDF.show()
}
