/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json.operation;

import java.io.Serializable;

import net.sf.json.JSON;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public interface JSONPathResolver extends Serializable
{
    Object resolve(JSON object, String path);
}
