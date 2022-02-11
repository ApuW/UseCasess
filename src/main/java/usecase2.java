import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class usecase2 {
    public static long getCount(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        Dataset<Row> Dormant_Customers = spark.sql("select c.*\n" +
                "from orders o right outer join customers c\n" +
                "on o.order_customer_id = c.customer_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_customer_id is null \n" +
                "order by c.customer_id");

        return Dormant_Customers.count();
    }
    static final Logger logger = Logger.getLogger(usecase2.class);
    public static void main(String[] args) {
        logger.info("**************************************************************// Spark Session Started \\************************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("**************************************************************// Orders Data \\************************************************");
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        orders.show(3);

        logger.info("**************************************************************// Customers Data \\************************************************");
        String path_customers = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_customers);
        customers.show(3);

        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");

        Dataset<Row> Dormant_Customers = spark.sql("select c.*\n" +
                "from orders o right outer join customers c\n" +
                "on o.order_customer_id = c.customer_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_customer_id is null \n" +
                "order by c.customer_id");
        logger.info("*************************************************************// Output of Dormant Customers \\***********************************************************");
        Dormant_Customers.show();
        String path = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\Output\\usecase2";
        Dormant_Customers.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
        logger.info("*************************************************************// Spark Session ended \\***********************************************");
    }
}
