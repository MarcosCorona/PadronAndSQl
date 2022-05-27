import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Exercises extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Padron.06")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //6.1)
  //Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el
  //csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que
  //ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y
  //que infiera el esquema

  import spark.implicits._

  val padron = spark.read.format("csv")
    .option("header","true")
    .option("delimiter", ";")
    .option("inferSchema","true")
    .option("emptyValue","0")
    .load("Rango_Edades_Seccion_202205.csv")
    .withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
    .withColumn("DESC_BARRIO",trim(col("desc_barrio")))
  //padron.show()

  //6.3) Enumera todos los barrios diferentes.

  padron.select(countDistinct("desc_barrio").alias("n_barrios")).show()

  //6.4)
  //Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
  //diferentes que hay.

  padron.createOrReplaceTempView("padron")

  //ya que es una view lo vamos a consultar con sql
  spark.sql("select count(distinct(desc_barrio)) as n_barriosSQL from padron").show()

  //6.5) Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".

  val padron3 = padron.withColumn("longitud",length(col("desc_distrito")))

  padron3.show()

  //6.8) Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.

  val padron_particionado = padron.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

 //6.9) Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estadode los rdds almacenados.

  padron_particionado.cache()

  //6.10) Lanza una consulta contra el DF resultante en la que muestre el número total
  // de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres"
  // para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en
  // aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna
  // "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

  padron_particionado.groupBy(col("desc_barrio"),col("desc_distrito"))
    .agg(count(col("espanolesHombres")).alias("espanolesHombres"),
      count(col("espanolesMujeres")).alias("espanolesMujeres"),
      count(col("extranjerosHombres")).alias("extranjerosHombres"),
      count(col("extranjerosMujeres")).alias("extranjerosMujeres"))
    .orderBy(desc("extranjerosMujeres"),desc("extranjerosHombres"))
    .show()

  //6.11) Elimina el registro en caché.
    spark.catalog.clearCache()

  //6.12) Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO,
  // otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio.
  // Únelo (con un join) con el DataFrame original a través de las columnas en común.
  val df1 = padron_particionado.select(col("DESC_BARRIO"),col("DESC_DISTRITO"),col("ESPANOLESHOMBRES")) .groupBy(col("DESC_BARRIO"),col("DESC_DISTRITO"))
    .agg(sum(col("ESPANOLESHOMBRES")).alias("ESPANOLESHOMBRES"))
    .join(padron, padron_particionado("desc_barrio") === padron("desc_barrio") && padron_particionado("desc_distrito") === padron("desc_distrito"),"inner")
    .show()

  //6.13) Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).

  import org.apache.spark.sql.expressions.Window
  val padron_ventana = padron.withColumn("TotalEspHom", sum(col("espanoleshombres"))
    .over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))

  padron_ventana.show()

  //6.14) Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a este:

  val distritos = Seq("BARAJAS","CENTRO","RETIRO")

  val padron_pivot = padron_particionado
    .groupBy("cod_edad_int")
    .pivot("desc_distrito", distritos).sum("espanolesMujeres")
    .orderBy(col("cod_edad_int"))

  padron_pivot.show()

  //6.15) Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia
  // a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para
  // cada rango de edad representa cada uno de los tres distritos. Debe estar
  // redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en
  // ninguna columna auxiliar creada para el caso.
  val padron_porcen = padron_pivot
    .withColumn("porcentaje_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
    .withColumn("porcentaje_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
    .withColumn("porcentaje_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

  padron_porcen.show()

  //6.16) Guarda el archivo csv original particionado por distrito y
  // por barrio (en ese orden) en un directorio local. Consulta el directorio
  // para ver la estructura de los ficheros y comprueba que es la esperada.

  padron_particionado.write.format("csv")
    .option("header","true")
    .mode("overwrite")
    .partitionBy("desc_distrito","desc_barrio")
    .save("C://asd")
}
