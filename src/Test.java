import Wrangler.Wrangler;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Test {
    public static void main(String[] args){
        Wrangler wr = new Wrangler();
        wr.test();
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        try {
            
            engine.eval("dw.split().column(\"data\")\n" +
                    "                .table(0)\n" +
                    "                .status(\"active\")\n" +
                    "                .drop(true)\n" +
                    "                .result(\"column\")\n" +
                    "                .update(false)\n" +
                    "                .insert_position(\"right\")\n" +
                    "                .on(\",\")\n" +
                    "                .before(\"sdfsd\")\n" +
                    "                .after(\"ddss\")\n" +
                    "                .which(1)\n" +
                    "                .max(0);");
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }
}
