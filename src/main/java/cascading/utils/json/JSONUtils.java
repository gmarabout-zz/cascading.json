/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json;

import net.sf.json.JSON;
import net.sf.json.JSONObject;

/**
 * JSONUtils: utilities for manipulating JSON objects.
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONUtils {
	
	/**
	 * Returns a JSON object from the specified source.
	 * If <i>source</i> is a string, then the string is parsed, and the resulting
	 * JSON object is returned.
	 * If <i>source</i> is a JSONWritable, then the underlying JSONObject is returned.
	 */
	public static JSON getJSON(Object source) {
		if (source instanceof String) {
			return JSONObject.fromObject((String) source);
		} else if (source instanceof JSONWritable) {
			return ((JSONWritable) source).get();
		} else {
			throw new RuntimeException(
					"The JSON Splitter does not know how to handle an object of type: "
							+ source.getClass().getName());
		}
	}
}
