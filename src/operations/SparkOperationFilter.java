package operations;


import Wrangler.Wrangler;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class SparkOperationFilter extends SparkOpration {
    @Override
    public JavaRDD<String[]> execute(JavaSparkContext jsc, JavaRDD<String[]> data, WranglerOperation wranglerOperation,Wrangler wrangler) {
        int columnId = wrangler.getColumnId(wranglerOperation);
        switch (wranglerOperation.getParameter("conditions")) {
            case "is_null":
                String columnName = wranglerOperation.getParameter("lcol");
                columnId = wrangler.getColumnId(columnName);
                return filter(data, columnId);
            case "rowIndex":
                return filter_rowIndex(jsc, data, wranglerOperation.getParameter("indices"));
            case "eq":
                return filter(data, columnId, wranglerOperation.getParameter("value"));

        }
        return null;
    }

    private static JavaRDD<String[]> filter_rowIndex(JavaSparkContext jsc, JavaRDD<String[]> data, String indices) {
        String[] indecesList = indices.substring(1, indices.length() - 1).split(",");
        ArrayList<Integer> indecesList2 = new ArrayList<>();
        for (int i = 0; i < indecesList.length; i++) {
            indecesList2.add(Integer.parseInt(indecesList[i]));
        }
        Collections.sort(indecesList2);
        List<String[]> list = data.collect();
        //System.out.println(list.size());

        for (int i = indecesList2.size()-1; i >= 0; i--) {
            int t = indecesList2.get(i);
            list.remove(t);
        }
        //System.out.println(list.size());
        return jsc.parallelize(list);
    }

    private static JavaRDD<String[]> filter(JavaRDD<String[]> data, final int columnId, final String value) {
        return data.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] row) throws Exception {
                if (row[columnId]==null) return true;
                if (row[columnId].equals(value)) {
                    return false;
                } else {
                    return true;
                }
            }
        });
    }

    private static JavaRDD<String[]> filter(JavaRDD<String[]> data, final int columnId) {
        return data.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] row) throws Exception {
                if (row[columnId]==null) return false;
                else return true;
            }
        });
    }

}
