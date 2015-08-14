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

    private static JavaRDD<String[]> extract(JavaRDD<String[]> data,final int columnId, final String after, final String before, final String on) {
        System.out.println("Split - "+columnId+" "+after+" "+before+" "+on);
        return data.map(new Function<String[], String[]>() {
            @Override
            public String[] call(String[] row) throws Exception {
                if (row == null) return null;
                if (row[columnId]==null) {
                    String[] list = new String[row.length + 1];
                    for(int i=0,j=0;i<row.length;i++,j++){
                        if(i==columnId){
                            j++;
                            list[j]=null;
                        }else{
                            list[j] = row[i];
                        }
                    }
                    return list;
                } else {
                    String[] list = new String[row.length + 1];
                    for (int i = 0,j=0; i < row.length; i++,j++) {
                        if (columnId == i) {
                            String val = row[i];
                            Pattern pattern = Pattern.compile(after + on + before);
                            Matcher matcher = pattern.matcher(val);
                            if (matcher.find()) {
                                list[j] = val;
                                list[++j] = val.substring(matcher.start() - matcher.end());
                            } else {
                                list[++j] = null;
                            }

                        } else {
                            list[j] = row[i];
                        }
                    }
                    return list;
                }
            }
        });
    }
}
