package operations;

import Wrangler.Wrangler;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkOperationDrop extends SparkOpration{
    @Override
    public JavaRDD<String[]> execute(JavaSparkContext jsc, JavaRDD<String[]> data, WranglerOperation wranglerOperation, Wrangler wrangler) {
        String columnName = wranglerOperation.getParameter("column");
        int columnIndex = wrangler.removeColumn(columnName);
        return drop(data,columnIndex);
    }

    private static JavaRDD<String[]> drop(JavaRDD<String[]> data, final int columnId){
        return data.map(new Function<String[], String[]>() {
            @Override
            public String[] call(String[] row) throws Exception {
                String[] newRow = new String[row.length-1];
                for (int i = 0,j=0; i < row.length;j++, i++) {
                    if (i == columnId){
                        i++;
                        if(i==row.length) return newRow;
                    }
                    newRow[j] = row[i];
                }
                return newRow;
            }
        });
    }


}
