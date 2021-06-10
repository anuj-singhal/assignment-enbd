
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SQLContext, SparkSession }
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import java.nio.file.Files

object TestApp extends App {
    val spark = SparkSession.builder.master("local")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
    
    import spark.implicits._

  implicit val sqlCtx: SQLContext = spark.sqlContext

  val schemaAcc: StructType = new StructType()
    .add("AccountId", DataTypes.LongType)
    .add("AccountType", DataTypes.IntegerType)

  val schemaLoan: StructType = new StructType()
    .add("LoanId", DataTypes.LongType)
    .add("AccountId", DataTypes.LongType)
    .add("Amount", DataTypes.DoubleType)
    
  val accountStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "demo-topic")
    .load()
    
    val loanStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "demo-topic2")
    .load()
    
    val accountDS = accountStream.selectExpr("CAST(value AS STRING) as val", "ts")
                           .select(col("ts").cast("timestamp"), from_json(col("val"), schemaAcc).as("values"))
                           .selectExpr("val.AccountId", "val.AccountType", "accts")
        
    val loanDS = loanStream.selectExpr("CAST(value AS STRING) as val", "ts")
                           .select(col("ts").cast("timestamp"), from_json(col("val"), schemaLoan).as("values"))
                           .selectExpr("values.LoanId", "values.AccountId as LAccountId", "values.Amount", "loants")
                           
    val accLoanJoinDF = loanDS.join(accountDS, expr("""loanDS.LAccountId = accountDS.AccountId AND  loants >= accts AND loants <= accts"""), joinType = "leftOuter")

    
    val query = accLoanJoinDF.writeStream
                              .outputMode("complete")
                              .format("console")
                              .start()

    query.awaitTermination()
    
}