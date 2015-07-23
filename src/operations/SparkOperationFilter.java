package operations;


import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.util.*;

public class SparkOperationFilter extends SparkOpration {
    @Override
    public JavaRDD<Row> execute(JavaSparkContext jsc, JavaRDD<Row> data, WranglerOperation wranglerOperation) {
        HashMap<String, String> parameters = wranglerOperation.getParameters();
        switch (parameters.get("conditions")) {
            case "is_null":
                return filter(data, 0);
            case "rowIndex":
                return filter_rowIndex(jsc,data, parameters.get("indices"));
            case "eq":
                return filter(data, 0, parameters.get("value"));

        }
        return null;
    }

    private static JavaRDD<Row> filter_rowIndex(JavaSparkContext jsc, JavaRDD<Row> data, String indices) {
        String[] indecesList = indices.substring(1, indices.length() - 1).split(",");
        ArrayList<Integer> indecesList2 = new ArrayList<>();
        for (int i = 0; i < indecesList.length; i++) {
            indecesList2.add(Integer.parseInt(indecesList[i]));
        }
        Collections.sort(indecesList2);
        List<Row> list = data.collect();
        //System.out.println(list.size());

        for (int i = indecesList2.size()-1; i >= 0; i--) {
            int t = indecesList2.get(i);
            list.remove(t);
        }
        //System.out.println(list.size());
        return jsc.parallelize(list);
    }

    private static JavaRDD<Row> filter(JavaRDD<Row> data, final int columnId, final String value) {
        return data.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.isNullAt(columnId)) return true;
                if (row.get(columnId).equals(value)) {
                    return false;
                } else {
                    return true;
                }
            }
        });
    }

    private static JavaRDD<Row> filter(JavaRDD<Row> data, final int columnId) {
        return data.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.isNullAt(columnId)) return false;
                else return true;
            }
        });
    }

}
