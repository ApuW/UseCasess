import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class usecase5 {
    public static long getCount() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String path_departments = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\departments\\part-00000";
        Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_departments);
        String path_products = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_products);
        String path_categories = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_categories);
        departments.createOrReplaceTempView("departments");
        categories.createOrReplaceTempView("categories");
        products.createOrReplaceTempView("products");
        Dataset<Row> Product_Count_Per_Department = spark.sql("select d.department_id, d.department_name, count(*) as product_count\n" +
                "from departments d join categories c\n" +
                "on d.department_id = c.category_department_id\n" +
                "join products p\n" +
                "on p.product_category_id = c.category_id\n" +
                "where p.product_category_id = c.category_id\n" +
                "group by d.department_id, d.department_name\n" +
                "order by d.department_id");
        return Product_Count_Per_Department.count();
    }

    static final Logger logger = Logger.getLogger(usecase5.class);
    public static void main(String[] args) {
        logger.info("**************************************************************// Spark Session Started \\************************************************");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("**************************************************************// Departments Data \\************************************************");
        String path_departments = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\departments\\part-00000";
        Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path_departments);
        departments.show(3);

        logger.info("**************************************************************// Products Data \\************************************************");
        String path_products = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_products);
        products.show(3);

        logger.info("**************************************************************// Categories Data \\************************************************");
        String path_categories = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCase\\src\\main\\resources\\retail_db\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("csv").option("header",true).option("inferSchema",true).load(path_categories);
        categories.show(3);

        departments.createOrReplaceTempView("departments");
        categories.createOrReplaceTempView("categories");
        products.createOrReplaceTempView("products");

        Dataset<Row> Product_Count_Per_Department = spark.sql("select d.department_id, d.department_name, count(*) as product_count\n" +
                "from departments d join categories c\n" +
                "on d.department_id = c.category_department_id\n" +
                "join products p\n" +
                "on p.product_category_id = c.category_id\n" +
                "where p.product_category_id = c.category_id\n" +
                "group by d.department_id, d.department_name\n" +
                "order by d.department_id");
        logger.info("*************************************************************// Output of Product Count Per Department \\***********************************************************");
        Product_Count_Per_Department.show();

        String path = "C:\\Users\\Apurva Waghmode\\IdeaProjects\\UseCasess\\src\\main\\Output\\usecase5";
        Product_Count_Per_Department.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
        logger.info("*************************************************************// Spark Session ended \\*****************************************************");

    }
}