/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json;

import net.sf.json.JSONObject;

import java.io.Serializable;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public interface JSONPathResolver extends Serializable
{
    Object resolve(JSONObject object, String path);
}
