/*
 * Copyright 2009, Grégoire Marabout. All rights reserved.
 */
package cascading.utils.json;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import net.sf.json.JSONObject;

/**
 * A filter class that works on JSON objects to filter tuple that does not
 * respect the contract specified. The contract is a couple of path/value,
 * implemented as two arrays in the constructor.
 *
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONFilter extends JSONOperation implements Filter {

  private Object[] values;

  public JSONFilter(String path, Object value){
    super( Fields.ALL, path );
    this.values = new Object[]{ value };
  }

  public JSONFilter(String[] paths, Object[] values){
    super( Fields.ALL, paths );
    this.values = values;
  }

  public JSONFilter(Fields fieldDeclaration, JSONPathResolver resolver, String[] paths, Object[] values){
    super( fieldDeclaration, resolver, paths );
    this.values = values;
  }

  protected void validate(){
    assert values != null && values.length == getPaths().length;
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
    JSONObject jsonObj = (JSONObject) filterCall.getArguments().get( 0 );
    return isFiltered( jsonObj );
  }

  boolean isFiltered(JSONObject jsonObj){
    String[] paths = getPaths();
    for ( int i = 0; i < paths.length; i++ ) {
      Object value = getValue( jsonObj, paths[i] );
      if (value == null || !value.equals( values[i] ) ) {
        return true;
      }
    }
    return false;
  }
}
