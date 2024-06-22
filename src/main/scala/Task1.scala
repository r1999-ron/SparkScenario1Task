import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{avg, sum, max}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object taskt1 {
  val config = ConfigFactory.load()
  private val awsAccessKeyId = config.getString("aws.access-key-id")
  private val awsSecretAccessKey = config.getString("aws.secret-access-key")
  private val awsS3Endpoint = config.getString("aws.endpoint")

  def main(args: Array[String]): Unit = {
    val tableName = "air_quality_table"
    val keySpaceName = "mykeyspaceronak"
    val df = readFromS3(getSpark("readS3"), "s3a://newbucketronak/zaragoza_data.csv")
    writeToKeySpace(df, tableName, keySpaceName)
    val readFromKeySpaceDf = readFromKeySpace(getSpark("KeyspaceToParquet"), tableName, keySpaceName)
    writeToS3(readFromKeySpaceDf, "s3a://newbucketronak/air-quality-parquet/")
    aggregationOnParquet(getSpark("aggregation"), "s3a://newbucketronak/air-quality-parquet/")
  }

  private def getSpark(name: String): SparkSession = {
    SparkSession.builder
      .appName(name)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.port", "7077")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.master", "local[10]")
      .config("spark.cassandra.connection.host", "cassandra.eu-north-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "ronak-at-851725165485")
      .config("spark.cassandra.auth.password", "ZN0Vhut4sUMe35YZAUEf8kWydW1yIg3eM1G7Zw+6OUHvd7190PfvWpvUf/M=")
      .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/ronak/Downloads/Cass/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "Ron@1999")
      .getOrCreate()
  }

  private def readFromS3(spark: SparkSession, csvFilePath: String): DataFrame = {

    println("Ronak :: trying reading from S3")

    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.s3a.access.key", awsAccessKeyId)
    hadoopConfig.set("fs.s3a.secret.key", awsSecretAccessKey)
    hadoopConfig.set("fs.s3a.endpoint", awsS3Endpoint)
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    val df = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)
    println("Ronak :: read from S3 " + df.show())
    df
  }

  private def writeToKeySpace(df: DataFrame, tableName: String, keySpaceName: String): Unit = {
    println("Ronak :: writing to keySpace")
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
      .mode("append")
      .save()
    println("Ronak :: written to KeySpace")
   }

  private def readFromKeySpace(sparkSession: SparkSession, tableName: String, keySpaceName: String): DataFrame = {
     println("Ronak :: trying reading from keyspace")
     val df = sparkSession.read
       .format("org.apache.spark.sql.cassandra")
       .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
       .load()
     println("Ronak :: fetched from keyspace")
     df
   }

   def renameColumns(df: DataFrame): DataFrame = {
        val renamedCols = df.columns.foldLeft(df) { (tempDF, colName) =>
        tempDF.withColumnRenamed(colName, colName.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_"))
        }
        renamedCols
    }
    
   private def writeToS3(df: DataFrame, parquetOutputPath: String): Unit = {
     val renamedDf = renameColumns(df)
     println("Ronak :: reached here to write in s3")
     renamedDf.write.parquet(parquetOutputPath)
   }

   private def aggregationOnParquet(spark: SparkSession, parquetInputPath: String) = {
     println("Ronak :: reached here")
     val parquetDF = spark.read.parquet(parquetInputPath)

    // 1. Calculate the average NO2 levels per station
    val avgNO2PerStation = parquetDF.groupBy("station_name")
      .agg(avg("NO2").alias("avg_NO2"))
    avgNO2PerStation.show()

     // 2. Calculate the maximum O3 levels per station
    val maxO3PerStation = parquetDF.groupBy("station_name")
      .agg(max("O3").alias("max_O3"))
    maxO3PerStation.show()

    // 3. Calculate the total PM10 levels per station
    val totalPM10PerStation = parquetDF.groupBy("station_name")
      .agg(sum("PM10").alias("total_PM10"))
    totalPM10PerStation.show()

    // 4. Calculate the average temperature per station
    val avgTempPerStation = parquetDF.groupBy("station_name")
      .agg(avg("Temp").alias("avg_Temp"))
    avgTempPerStation.show()
   }
}




