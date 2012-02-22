package cascading.json.operation;

import java.util.Iterator;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import cascading.flow.FlowProcess;
import cascading.json.JSONUtils;
import cascading.json.JSONWritable;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class JSONFlatten extends JSONOperation implements Function {

	private String[] paths;

	public JSONFlatten(Fields fieldDeclaration, String... paths) {
		super(fieldDeclaration);
		this.paths = paths;
	}

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		JSON json = JSONUtils.getJSON(functionCall.getArguments().get(0));

		if (json.isArray()) {
			JSONArray jsonArray = (JSONArray) json;
			// jsonArray must be a JSONArray of JSONObjects!
			for (Iterator iter = jsonArray.iterator(); iter.hasNext();) {
				Object item = iter.next();
				assert item instanceof JSONObject;

				Tuple tuple = new Tuple();
				for (String path : paths) {
					Comparable value = getValue((JSON) item, path);
					if (value instanceof JSON)
						value = new JSONWritable((JSON) value);
					tuple.add(value);
				}
				functionCall.getOutputCollector().add(tuple);
			}
		}

	}

}
