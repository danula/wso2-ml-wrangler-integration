package operations;

import Wrangler.Wrangler;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import java.io.Serializable;

public abstract class SparkOpration implements Serializable {
    String name;
    public abstract JavaRDD<String[]> execute(JavaSparkContext jsc, JavaRDD<String[]> data, WranglerOperation wranglerOperation,Wrangler wrangler);
}
