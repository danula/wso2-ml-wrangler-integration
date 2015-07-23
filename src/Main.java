import Wrangler.WranglerOperation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

// Import factory methods provided by DataType.
import org.apache.spark.sql.api.java.DataType;

// Import StructType and StructField
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.api.java.StructField;

// Import Row.
import org.apache.spark.sql.api.java.Row;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.regex.*;
import java.util.ArrayList;
import java.util.List;


public final class Main {
    public static JavaSparkContext jsc;
    static int numberOfColumns;
    static WranglerOperation wo;

    public static void main(String[] args) throws Exception {

        jsc = new JavaSparkContext("local", "JavaSparkPi");

        numberOfColumns = 2;

        // reading wrangler script
        Path path = Paths.get("script1.js");
        Scanner scanner = new Scanner(path);
        boolean flag = false;

        wo = new WranglerOperation();

        while(scanner.hasNextLine()){
            String line = scanner.nextLine();
            flag = parseLine(line,flag,wo);
        }



        // The schema is encoded in a string
        String schemaString = "col1 col2";

        // Generate the schema based on the string of schema
        final List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataType.createStructField(fieldName, DataType.StringType, true));
        }
        StructType schema = DataType.createStructType(fields);

        JavaRDD<String> data = jsc.textFile("data.txt");

        //Split data by ',' and save as rows
        JavaRDD<Row> rowRDD = data.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] split = record.split(",");
                        String[] records = new String[numberOfColumns];
                        for (int i = 0; i < numberOfColumns; i++) {
                            if (i < split.length && !split[i].equals("")) {
                                records[i] = split[i];
                            } else {
                                records[i] = null;
                            }
                        }
                        if (records.length > 0) {
                            return Row.create(records);
                        }
                        return null;
                    }
                });

        printRow(rowRDD.collect().get(6));
        printRow(rowRDD.collect().get(7));
        printRow(rowRDD.collect().get(8));
        System.out.println(rowRDD.count());
        JavaRDD<Row> rowRDD4 = wo.executeOperation(jsc,rowRDD);
        //JavaRDD<Row> rowRDD4 = wo.executeOperation(jsc,rowRDD);
        //JavaRDD<Row> rowRDD3 = filter(rowRDD, 1, "4029.3");
        //JavaRDD<Row> rowRDD4 = split(rowRDD, 0, " in ", "[a-zA-Z]+", ".*");
        //JavaRDD<Row> rowRDD4 = extract(rowRDD, 0, ".*","","crime ");
        //JavaRDD<Row> rowRDD5 = fillColumn(rowRDD, 1, "below");
        //JavaRDD<Row> rowRDD6 = fillColumn(rowRDD, 0, "right");
        //System.out.println(rowRDDP.first());
        //System.out.println(rowRDD.collect().get(2).get(14));
        printRow(rowRDD4.collect().get(6));
        printRow(rowRDD4.collect().get(7));
        printRow(rowRDD4.collect().get(8));
        //printRow(rowRDD4.collect().get(1));
        //printRow(rowRDD6.collect().get(3));
        System.out.println(rowRDD4.count());

        jsc.stop();
    }





    private static void printRow(Row row) {
        for (int i = 0; i < row.length(); i++) {
            System.out.print(row.get(i) + "  ");
        }
    }

    private static boolean parseLine(String line,boolean flag,WranglerOperation wo){
        Pattern pattern;
        Matcher matcher;

        line = line.trim();
        if(line.equals(")")) {
            System.out.println("##############");
            return false;
        }

        if(line.startsWith("w.add(")){
            pattern = Pattern.compile("\\.[a-z_]+\\(");
            matcher = pattern.matcher(line);
            matcher.find();
            matcher.find();
            String operation = matcher.group();
            operation = operation.substring(1,operation.length()-1);
            System.out.println("+++++"+operation+"++++++++");
            wo.setOperation(operation);
            flag = true;
            line = line.substring(9);
        }
        if(line.matches(".*dw\\.[a-zA-Z_]+\\(.*")){
            String l1 = line.substring(1,line.indexOf('('));
            System.out.println("+++++"+l1+"++++++++");
            pattern = Pattern.compile("\\.[a-zA-Z_]+\\(");
            matcher = pattern.matcher(line);
            matcher.find();
            matcher.find();
            String operation = matcher.group();
            operation = operation.substring(1, operation.length()-1);
            System.out.println("+++++"+operation+"++++++++");
            wo.addParameter(l1,operation);

            line = line.replaceAll(".*dw\\.[a-zA-Z_]+\\(","");

        }

        if(flag){
            pattern = Pattern.compile("\\.[a-zA-Z_]+\\(");
            matcher = pattern.matcher(line);
            if(matcher.find()){
                String param = matcher.group();
                param = param.substring(1,param.length()-1);
                line = line.substring(matcher.end());

                if(line.matches(".*dw\\.[a-zA-Z_]+\\(.*")){

                    System.out.println(line);
                }
                String value = line.substring(matcher.groupCount(),line.length()-1);
                if(!value.equals("undefined")){
                    wo.addParameter(param, value);
                }
            }

        }
        return flag;
    }
}