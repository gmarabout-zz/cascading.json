/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json.operation;

import net.sf.json.JSON;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.utils.json.JSONUtils;
import cascading.utils.json.JSONWritable;

/**
 * A class to parse a JSON string and outputs a JSON object.
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONParser extends BaseOperation implements Function {

    public JSONParser(Fields fieldDeclaration){
        super( fieldDeclaration );    
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall){
        JSON json = JSONUtils.getJSON(functionCall.getArguments().getString( 0 ));
        JSONWritable jsonWritable = new JSONWritable(json);
    	functionCall.getOutputCollector().add( new Tuple( jsonWritable ) );
    }
}
