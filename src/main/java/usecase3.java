import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class usecase3 {
    public static long getCount(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_customers = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_customers);
        String path_order_items = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_order_items);

        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        order_items.createOrReplaceTempView("order_items");
        Dataset<Row> Revenue_Per_Customer = spark.sql("select c.customer_id, c.customer_fname,c.customer_lname, " +
                "coalesce(round(sum(oi.order_item_subtotal),2),0) AS customer_revenue\n" +
                "from orders o right outer join customers c\n" +
                "on o.order_customer_id = c.customer_id \n" +
                "join order_items oi\n" +
                "on oi.order_item_order_id = o.order_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "group by c.customer_id, c.customer_fname, c.customer_lname\n" +
                "order by customer_revenue desc,c.customer_id");
        return Revenue_Per_Customer.count();
    }
    static final Logger logger = Logger.getLogger(usecase3.class);
    public static void main(String[] args) {
        logger.info("**************************************************************// Spark Session Started \\************************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("**************************************************************// Orders Data \\************************************************");
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\usecases\\src\\main\\resources\\retail_db\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        orders.show(3);

        logger.info("**************************************************************// Customers Data \\************************************************");
        String path_customers = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\usecases\\src\\main\\resources\\retail_db\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_customers);
        customers.show(3);

        logger.info("**************************************************************// Order Items Data \\************************************************");
        String path_order_items = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_order_items);
        order_items.show(3);

        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        order_items.createOrReplaceTempView("order_items");

        Dataset<Row> Revenue_Per_Customer = spark.sql("select c.customer_id, c.customer_fname,c.customer_lname, " +
                "coalesce(round(sum(oi.order_item_subtotal),2),0) AS customer_revenue\n" +
                "from orders o right outer join customers c\n" +
                "on o.order_customer_id = c.customer_id \n" +
                "join order_items oi\n" +
                "on oi.order_item_order_id = o.order_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "group by c.customer_id, c.customer_fname, c.customer_lname\n" +
                "order by customer_revenue desc,c.customer_id");
        logger.info("*************************************************************// Output of Revenue Per Customer \\***********************************************************");
        Revenue_Per_Customer.show();
        String path = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\Output\\usecase3";
        Revenue_Per_Customer.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
        logger.info("*************************************************************// Spark Session ended \\***********************************************");

    }
}