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
            
            engine.eval("print('Hello')" );
        } catch (ScriptException e) {
            e.printStackTrace();
        }

    }
}
