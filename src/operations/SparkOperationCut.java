package operations;

import Wrangler.Wrangler;
import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkOperationCut extends SparkOpration{

    @Override
    public JavaRDD<String[]> execute(JavaSparkContext jsc, JavaRDD<String[]> data, WranglerOperation wranglerOperation, Wrangler wrangler) {
        int columnId = wrangler.getColumnId(wranglerOperation.getParameter("column"));
        String after = wranglerOperation.getParameter("after");
        String before  = wranglerOperation.getParameter("before");
        String on = wranglerOperation.getParameter("on");
        String positions = wranglerOperation.getParameter("positions");
        if(after==null&&before==null&&on==null) return cutOnIndex(data, columnId, positions);
        if(after==null) after="";
        if(before==null) before="";
        if(on==null) on = "";
        return cut(data, columnId, after, before, on);
    }
    private JavaRDD<String[]> cutOnIndex(JavaRDD<String[]> data,final int columnId,final String positions) {
        return data.map(new Function<String[], String[]>() {
                            @Override
                            public String[] call(String[] row) throws Exception {
                                String[] newRow = new String[row.length+1];
                                if (row[columnId]==null) {
                                    return row;
                                } else {
                                    for (int i = 0; i < row.length; i++) {
                                        if (columnId == i) {
                                            String val = row[i];
                                            String positions1 = positions.substring(1,positions.length()-1);
                                            String[] positions2 = positions1.split(",");
                                            int p1 = Integer.parseInt(positions2[0]);
                                            int p2 = Integer.parseInt(positions2[1]);
                                            newRow[i] = val;
                                            if(p2<val.length()) {
                                                newRow[i] = val.substring(0, p1).concat(val.substring(p2,val.length()));
                                            }else if(p1<val.length()){
                                                newRow[i] = val.substring(0,p1);
                                            }else{
                                                newRow[i] = row[i];
                                            }
                                        } else {
                                            newRow[i] = row[i];
                                        }
                                    }
                                    return newRow;
                                }
                            }
                        }
        );
    }

    private static JavaRDD<String[]> cut(JavaRDD<String[]> data,final int columnId, final String after, final String before, final String on) {
        System.out.println("Split - "+columnId+" "+after+" "+before+" "+on);
        return data.map(new Function<String[], String[]>() {
            @Override
            public String[] call(String[] row) throws Exception {
                if (row == null) return null;
                if (row[columnId]==null) {
                    return row;
                } else {
                    String[] newRow = new String[row.length + 1];
                    for (int i = 0; i < row.length; i++) {
                        if (columnId == i) {
                            String val = row[i];
                            Pattern pattern = Pattern.compile(after + on + before);
                            Matcher matcher = pattern.matcher(val);
                            if (matcher.find()) {
                                newRow[i] = val.substring(0,matcher.start()).concat(val.substring(matcher.end(),val.length()));
                            } else {
                                newRow[i] = row[i];
                            }
                        } else {
                            newRow[i] = row[i];
                        }
                    }
                    return newRow;
                }
            }
        });
    }
}
