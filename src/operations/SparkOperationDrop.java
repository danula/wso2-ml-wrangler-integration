package operations;

import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.util.HashMap;

public class SparkOperationDrop extends SparkOpration{
    @Override
    public JavaRDD<Row> execute(JavaSparkContext jsc, JavaRDD<Row> data, WranglerOperation wranglerOperation) {
        HashMap<String, String> parameters = wranglerOperation.getParameters();
        return drop(data,0);
    }

    private static JavaRDD<Row> drop(JavaRDD<Row> data, final int columnId){
        return data.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String[] rowElements = new String[row.length()];
                for (int i = 0; i < row.length(); i++) {
                    if (i == columnId) i++;
                    if (row.isNullAt(i)) {
                        rowElements[i] = null;
                    } else {
                        rowElements[i] = row.getString(i);
                    }
                }
                return Row.create(rowElements);
            }
        });
    }


}
