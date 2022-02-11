import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class usecase4 {
    public static long getCount(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        String path_products = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_products);
        orders.createOrReplaceTempView("orders");
        String path_order_items = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_order_items);
        String path_categories = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_categories);
        orders.createOrReplaceTempView("orders");
        categories.createOrReplaceTempView("categories");
        order_items.createOrReplaceTempView("order_items");
        products.createOrReplaceTempView("products");
        Dataset<Row> Revenue_Per_Category = spark.sql("select c.category_id, c.category_department_id, c.category_name, round(sum(oi.order_item_subtotal),2) as category_revenue\n" +
                "from orders o join order_items oi\n" +
                "on o.order_id = oi.order_item_order_id\n" +
                "join products p\n" +
                "on p.product_id = oi.order_item_product_id\n" +
                "join categories c\n" +
                "on c.category_id = p.product_category_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "group by c.category_id, c.category_department_id, c.category_name\n" +
                "order by c.category_id");
        return Revenue_Per_Category.count();
    }
    static final Logger logger = Logger.getLogger(usecase4.class);
    public static void main(String[] args) {
        logger.info("**************************************************************// Spark Session Started \\************************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("**************************************************************// Orders Data \\************************************************");
        String path_orders = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\usecases\\src\\main\\resources\\retail_db\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_orders);
        orders.show(3);

        logger.info("**************************************************************// Products Data \\************************************************");
        String path_products = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_products);
        products.show(3);

        logger.info("**************************************************************// Order Items Data \\************************************************");
        String path_order_items = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_order_items);
        order_items.show(3);

        logger.info("**************************************************************// Categories Data\\************************************************");
        String path_categories = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_categories);
        categories.show(3);

        orders.createOrReplaceTempView("orders");
        categories.createOrReplaceTempView("categories");
        order_items.createOrReplaceTempView("order_items");
        products.createOrReplaceTempView("products");

        Dataset<Row> Revenue_Per_Category = spark.sql("select c.category_id, c.category_department_id, c.category_name, round(sum(oi.order_item_subtotal),2) as category_revenue\n" +
                "from orders o join order_items oi\n" +
                "on o.order_id = oi.order_item_order_id\n" +
                "join products p\n" +
                "on p.product_id = oi.order_item_product_id\n" +
                "join categories c\n" +
                "on c.category_id = p.product_category_id\n" +
                "where o.order_date LIKE '2014-01%' and o.order_status in ('COMPLETE', 'CLOSED')\n" +
                "group by c.category_id, c.category_department_id, c.category_name\n" +
                "order by c.category_id");
        logger.info("*************************************************************// Output of Revenue Per Category \\***********************************************************");
        Revenue_Per_Category.show();
        String path = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\Output\\usecase4";
        Revenue_Per_Category.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
        logger.info("*************************************************************// Spark Session ended \\*****************************************************");

    }
}