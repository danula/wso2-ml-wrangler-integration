package operations;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkOperationSplit extends SparkOpration {
    @Override
    public JavaRDD<Row> execute(JavaSparkContext jsc, JavaRDD<Row> data, WranglerOperation wranglerOperation) {
        HashMap<String, String> parameters = wranglerOperation.getParameters();
        return split(data,0,parameters.get("after"),parameters.get("before"),parameters.get("on"));
    }
    private static JavaRDD<Row> split(JavaRDD<Row> data,final int columnId, final String after, final String before, final String on) {
        System.out.println("Split - "+columnId+" "+after+" "+before+" "+on);
        return data.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row == null) return null;
                if (row.isNullAt(columnId)) {
                    String[] list = new String[row.length() + 1];
                    for(int i=0,j=0;i<row.length();i++,j++){
                        if(row.isNullAt(i)){
                            list[j] = null;
                            if(i==columnId){
                                j++;
                                list[j]=null;
                            }
                        }else{
                            list[j] = row.getString(i);
                        }
                    }

                    return Row.create(list);
                } else {
                    String[] list = new String[row.length() + 1];
                    for (int i = 0,j=0; i < row.length(); i++,j++) {
                        if (columnId == i) {
                            String val = row.getString(i);
                            Pattern pattern = Pattern.compile(after + on + before);
                            Matcher matcher = pattern.matcher(val);
                            if (matcher.find()) {
                                list[j] = val.substring(0, matcher.start() + after.length());
                                list[++j] = val.substring(matcher.start() + after.length());
                            } else {
                                list[++j] = null;
                            }

                        } else {
                            if(row.isNullAt(i)){
                                list[j] = null;
                            }else {
                                list[j] = row.getString(i);
                            }
                        }
                    }
                    return Row.create(list);
                }
            }
        });
    }


}
