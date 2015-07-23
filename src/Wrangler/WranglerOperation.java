package Wrangler;

import operations.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import java.util.HashMap;


public class WranglerOperation {
    HashMap<String,String> paramters= new HashMap<>();
    String operation;

    public String getOperation(){
        return operation;
    }
    public void setOperation(String operation){
        this.operation = operation;
    }

    public HashMap<String, String> getParameters(){
        return paramters;
    }
    public String getParameter(String parameter){
        return paramters.get(parameter);
    }

    public void addParameter(String param,String value){
        if(value.matches("\".*\"")){
            value = value.substring(1,value.length()-1);
        }
        paramters.put(param,value);
        System.out.println(param + "\t" + value);
    }

    public JavaRDD<Row> executeOperation(JavaSparkContext jsc, JavaRDD<Row> data){
        SparkOpration so;
        switch (this.getOperation()){
            case "split" :
                so = new SparkOperationSplit();
                break;
            case "fill":
                so = new SparkOperationFill();
                break;
            case "filter":
                so = new SparkOperationFilter();
                break;
            case "drop":
                so = new SparkOperationDrop();
                break;
            default:
                so = null;
        }
        return so.execute(jsc,data,this);
    }

    public WranglerOperation result(String result){
        return this;
    }

    public WranglerOperation update(boolean b) {
        return this;
    }

    public WranglerOperation insert_position(String position) {
        return this;
    }

    public WranglerOperation on(String on) {
        return this;
    }

    public WranglerOperation before(String before) {
        return this;
    }

    public WranglerOperation after(String after) {
        return this;
    }

    public WranglerOperation which(int which) {
        return this;
    }

    public WranglerOperation max(int max) {
        return this;
    }

    public WranglerOperation fill() {
        return this;
    }

    public WranglerOperation direction(String direction) {
        return this;
    }

    public WranglerOperation method(String method) {
        return this;
    }
}
