package operations;

import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

public abstract class SparkOpration {
    String name;
    public abstract JavaRDD<Row> execute(JavaSparkContext jsc, JavaRDD<Row> data, WranglerOperation wranglerOperation);
}
