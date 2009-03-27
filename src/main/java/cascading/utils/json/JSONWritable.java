/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.io.WritableComparable;

/**
 * A Writable implementation for JSON objects.
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONWritable implements WritableComparable<JSONWritable> {

	private JSON json;

	public JSONWritable() {
	}

	public JSONWritable(JSON json) {
		this.json = json;
	}

	public JSON get() {
		return this.json;
	}

	@Override
	public String toString() {
		return json.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(json.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String jsonStr = in.readUTF();
		this.json = JSONSerializer.toJSON(jsonStr);
	}

	public static JSONWritable read(DataInput in) throws IOException {
		JSONWritable w = new JSONWritable();
		w.readFields(in);
		return w;
	}

	@Override
	public int compareTo(JSONWritable obj) {
		JSON jonObj = obj.get();		
		// Java lacks the duck typing feature :(
		if (this.json instanceof JSONObject)
			return ((JSONObject) this.json).compareTo(jonObj);
		if (this.json instanceof JSONArray)
			return ((JSONArray) this.json).compareTo(jonObj);
		if ((this.json instanceof JSONNull) && (jonObj instanceof JSONNull))
			return 0;
		return Integer.MAX_VALUE;
	}

}
