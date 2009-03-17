package cascading.utils.json;

import junit.framework.TestCase;
import net.sf.json.JSONObject;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONFilterTestCase extends TestCase {
  private static String JSON_DATA1 = "{ name: \"John\", data: { age: 33, address: { city: \"New York\"} }}";
  private static String JSON_DATA2 = "{ data: { age: 44, address: { city: \"Paris\"} }}";
  private static String JSON_DATA3 = "{ name: \"Michael\", data: { address: { city: \"London\"} }}";

  private static JSONObject jsonObj1, jsonObj2, jsonObj3;

  static{
    jsonObj1 = JSONObject.fromObject( JSON_DATA1 );
    jsonObj2 = JSONObject.fromObject( JSON_DATA2 );
    jsonObj3 = JSONObject.fromObject( JSON_DATA3 );
  }

  public void testIsFiltered(){
    JSONFilter filter = new JSONFilter( "data:age", 33 );
    assertFalse( filter.isFiltered( jsonObj1 ) );
    assertTrue( filter.isFiltered( jsonObj2 ) );
    assertTrue( filter.isFiltered( jsonObj3 ) );


    filter = new JSONFilter( new String[]{ "data:age" }, new Object[]{ 33 } );
    assertFalse( filter.isFiltered( jsonObj1 ) );
    assertTrue( filter.isFiltered( jsonObj2 ) );
    assertTrue( filter.isFiltered( jsonObj3 ) );

    filter = new JSONFilter( new String[]{ "data:age", "name" }, new Object[]{ 33, "John" } );
    assertFalse( filter.isFiltered( jsonObj1 ) );
    assertTrue( filter.isFiltered( jsonObj2 ) );
    assertTrue( filter.isFiltered( jsonObj3 ) );
  }
}
