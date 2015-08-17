package Wrangler;

import operations.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import java.util.ArrayList;
import java.util.HashMap;

public class WranglerOperation {
    HashMap<String,String> paramters;
    String operation;
    WranglerOperation nextOperation;

    public WranglerOperation(){
        nextOperation = null;
        paramters = new HashMap<>();
    }

    public void setNextOperation(WranglerOperation nextOperation) {
        this.nextOperation = nextOperation;
    }

    public WranglerOperation getNextOperation() {
        return nextOperation;
    }

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
        //System.out.println(param + "\t" + value);
    }

    public JavaRDD<String[]> executeOperation(JavaSparkContext jsc, JavaRDD<String[]> data,Wrangler wrangler){
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
            case "extract":
                so = new SparkOperationExtract();
                break;
            default:
                so = null;
        }
        return so.execute(jsc, data, this,wrangler);
    }

    public void printOperation(){
        System.out.println(operation);
        for(String k:paramters.keySet()){
            System.out.println(k+" - "+paramters.get(k));
        }
        System.out.println("================================");
    }


}
