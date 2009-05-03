package cascading.json.operation;

import net.sf.json.JSONObject;

import java.io.Serializable;


/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public abstract class JSONFilterConfig implements Serializable {
  private String[] paths;

  public JSONFilterConfig(String... paths){
    this.paths = paths;
  }

  protected String[] getPaths(){
    return paths;
  }

  /**
   * Method that returns <code>true</code> if the specification is respected, <code>false</code>
   * otherwise.
   *
   * @param obj
   * @param pathResolver
   * @return
   */
  public boolean respect(JSONObject obj, JSONPathResolver pathResolver){

    for ( int i=0; i<paths.length; i++) {
      Object value = pathResolver.resolve( obj, paths[i] );
      if ( !accept( value, i ) )
        return false;
    }
    return true;
  }

  protected abstract boolean accept(Object value, int index);

  //-----------------------------------------------------------------
  // Some concrete implementations

  public static class MustNotBeNull extends JSONFilterConfig {
    public MustNotBeNull(String[] paths){
      super( paths );
    }

    @Override
    protected boolean accept(Object value, int index){
      return value != null;
    }
  }

    public static class MustBeNull extends JSONFilterConfig {
    public MustBeNull(String[] paths){
      super( paths );
    }

    @Override
    protected boolean accept(Object value, int index){
      return value == null;
    }
  }

  public static class MustEqual extends MustNotBeNull {
    private Object[] values;

    public MustEqual(String[] paths, Object[] values){
      super( paths );
      this.values = values;  
    }

    @Override
    protected boolean accept(Object value, int index){
      Object refValue = values[index];
      return super.accept(value, index) && value.equals( refValue );
    }

  }

  public static class MustDiffer extends MustBeNull {
    private Object[] values;

    public MustDiffer(String[] paths, Object[] values){
      super( paths );
      this.values = values;
    }

    @Override
    protected boolean accept(Object value, int index){
      Object refValue = values[index];
      return super.accept( value, index )|| !value.equals( refValue );
    }
  }

}
