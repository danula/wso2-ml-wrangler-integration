package operations;

import Wrangler.Wrangler;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkOperationExtract extends SparkOpration{

    @Override
    public JavaRDD<String[]> execute(JavaSparkContext jsc, JavaRDD<String[]> data, WranglerOperation wranglerOperation, Wrangler wrangler) {
        return null;
    }

    private static JavaRDD<Row> extract(JavaRDD<Row> data,final int columnIndex,final String on, final String before,final String after){
        return data.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row == null) return null;
                if (row.isNullAt(columnIndex)) {
                    String[] list = new String[row.length() + 1];
                    for (int i = 0, j = 0; i <= row.length(); i++, j++) {
                        if (row.isNullAt(j)) {
                            list[i] = null;
                            if (i == columnIndex) {
                                i++;
                                list[i] = null;
                            }
                        } else {
                            list[i] = row.getString(j);
                        }
                    }

                    return Row.create(list);
                } else {
                    String[] list = new String[row.length() + 1];
                    for (int i = 0, j = 0; i <= row.length(); i++, j++) {
                        if (columnIndex == i) {
                            String val = row.getString(j);
                            Pattern pattern = Pattern.compile(after + on + before);
                            Matcher matcher = pattern.matcher(val);
                            System.out.println(matcher.toString());
                            if (matcher.find()) {
                                list[i] = val;
//                                System.out.println(matcher);
//                                System.out.println(val);
//                                System.out.println(matcher.start()+" "+after.length()+"  "+matcher.end()+" " +before.length());
                                list[i + 1] = val.substring(matcher.start() + after.length(), matcher.end() - before.length());
                            } else {
                                list[i + 1] = null;
                            }
                            i++;
                        } else {
                            if (row.isNullAt(j)) {
                                list[i] = null;
                            } else {
                                list[i] = row.getString(j);
                            }
                        }
                    }
                    return Row.create(list);
                }
            }
        });
    }
}
