package operations;

import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.util.HashMap;
import java.util.List;

public class SparkOperationFill extends SparkOpration{

    @Override
    public JavaRDD<Row> execute(JavaSparkContext jsc, JavaRDD<Row> data, WranglerOperation wranglerOperation) {
        HashMap<String, String> parameters = wranglerOperation.getParameters();
        return fillColumn(jsc,data,1,parameters.get("direction"));
    }

    private static JavaRDD<Row> fillColumn(JavaSparkContext jsc,JavaRDD<Row> data, final int columnIndex, final String direction) {
        if (direction.equals("left") || direction.equals("right")) {
            return data.map(new Function<Row, Row>() {
                @Override
                public Row call(Row r) throws Exception {
                    if (r.isNullAt(columnIndex)) {
                        String[] rowElements = new String[r.length()];
                        if (direction.equals("left")) {
                            if (r.isNullAt(0)) {
                                rowElements[0] = null;
                            } else {
                                rowElements[0] = r.getString(0);
                            }
                            for (int i = 1; i < r.length(); i++) {
                                if (r.isNullAt(i)) {
                                    if (i == columnIndex) {
                                        rowElements[i] = rowElements[i - 1];
                                    } else {
                                        rowElements[i] = null;
                                    }
                                } else {
                                    rowElements[i] = r.getString(i);
                                }
                            }

                        } else if (direction.equals("right")) {
                            if (r.isNullAt(r.length() - 1)) {
                                rowElements[r.length() - 1] = null;
                            } else {
                                rowElements[r.length() - 1] = r.getString(r.length() - 1);
                            }
                            for (int i = r.length() - 2; i >= 0; i--) {
                                if (r.isNullAt(i)) {
                                    if (i == columnIndex) {
                                        rowElements[i] = rowElements[i + 1];
                                    } else {
                                        rowElements[i] = null;
                                    }
                                } else {
                                    rowElements[i] = r.getString(i);
                                }
                            }
                        }
                        return Row.create(rowElements);
                    } else {
                        return r;
                    }
                }
            });

        } else if (direction.equals("above")) {
            List<Row> rows = data.collect();
            for (int i = 1; i < rows.size(); i++) {
                Row r = rows.get(i);
                if (r.isNullAt(columnIndex)) {
                    String[] rowElements = new String[r.length()];
                    for (int j = 0; j < r.length(); j++) {
                        if (j == columnIndex) {
                            if(rows.get(i - 1).isNullAt(columnIndex)){
                                rowElements[j]=null;
                            }else{
                                rowElements[j] = rows.get(i - 1).get(columnIndex).toString();
                            }

                        } else {
                            if(r.isNullAt(j)){
                                rowElements[j]=null;
                            }else{
                                rowElements[j] = r.getString(j);
                            }
                        }
                    }
                    rows.remove(i);
                    rows.add(i, Row.create(rowElements));
                }
            }
            return jsc.parallelize(rows);
        }else if(direction.equals("down")){
            List<Row> rows = data.collect();
            for (int i = rows.size()-2; i >= 0; i--) {
                Row r = rows.get(i);
                if (r.isNullAt(columnIndex)) {
                    String[] rowElements = new String[r.length()];
                    for (int j = 0; j < r.length(); j++) {
                        if (j == columnIndex) {
                            if(rows.get(i + 1).isNullAt(columnIndex)){
                                rowElements[j]=null;
                            }else{
                                rowElements[j] = rows.get(i + 1).get(columnIndex).toString();
                            }
                        } else {
                            if(r.isNullAt(j)){
                                rowElements[j]=null;
                            }else{
                                rowElements[j] = r.getString(j);
                            }
                        }
                    }
                    rows.remove(i);
                    rows.add(i, Row.create(rowElements));
                }
            }
            return jsc.parallelize(rows);
        }
        return null;
    }

    private static JavaRDD<Row> fillRow(JavaSparkContext jsc,JavaRDD<Row> data, int rowIndex, String direction) {
        List<Row> list = data.collect();
        Row r = list.get(rowIndex);
        String[] rowElements = new String[r.length()];
        if (direction.equals("left")) {
            if (r.isNullAt(0)) {
                rowElements[0] = null;
            } else {
                rowElements[0] = r.getString(0);
            }
            for (int i = 1; i < r.length(); i++) {
                if (r.isNullAt(i)) {
                    rowElements[i] = rowElements[i - 1];
                } else {
                    rowElements[i] = r.getString(i);
                }
            }
        } else if (direction.equals("right")) {
            if (r.isNullAt(r.length() - 1)) {
                rowElements[r.length() - 1] = null;
            } else {
                rowElements[r.length() - 1] = r.getString(r.length() - 1);
            }
            for (int i = r.length() - 2; i >= 0; i--) {
                if (r.isNullAt(i)) {
                    rowElements[i] = rowElements[i + 1];
                } else {
                    rowElements[i] = r.getString(i);
                }
            }
        }else if(direction.equals("above")){
            if(rowIndex==0) return data;
            for (int i = 0; i < r.length(); i++) {
                if(r.isNullAt(i)){
                    rowElements[i]=null;
                }else{
                    rowElements[i] = r.get(i).toString();
                }
            }
        }

        list.remove(rowIndex);
        list.add(rowIndex, Row.create(rowElements));
        return jsc.parallelize(list);

    }


}
