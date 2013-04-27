package cascading.json.operation;

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

  public void testKeepOnly(){
    JSONFilter filter = JSONFilter.mustEqual( "data:age", 33 );
    assertFalse( filter.isRemove( jsonObj1 ) );
    assertTrue( filter.isRemove( jsonObj2 ) );
    assertTrue( filter.isRemove( jsonObj3 ) );

    filter = JSONFilter.mustEqual( new String[]{ "data:age", "name" }, new Object[]{ 33, "John" } );
    assertFalse( filter.isRemove( jsonObj1 ) );
    assertTrue( filter.isRemove( jsonObj2 ) );
    assertTrue( filter.isRemove( jsonObj3 ) );
  }

  public void testExclude(){
    JSONFilter filter = JSONFilter.mustDiffer( "data:age", 33 );
    assertTrue( filter.isRemove( jsonObj1 ) );
    assertFalse( filter.isRemove( jsonObj2 ) );
    assertFalse( filter.isRemove( jsonObj3 ) );

    filter = JSONFilter.mustDiffer( new String[]{ "data:age", "name" }, new Object[]{ 33, "John" } );
    assertTrue( filter.isRemove( jsonObj1 ) );
    assertFalse( filter.isRemove( jsonObj2 ) );
    assertFalse( filter.isRemove( jsonObj3 ) );

    
    filter = JSONFilter.mustDiffer( new String[]{ "data:age", "data:age" }, new Object[]{ 33, 44 } );
    assertTrue( filter.isRemove( jsonObj1 ) );
    assertTrue( filter.isRemove( jsonObj2 ) );
    assertFalse( filter.isRemove( jsonObj3 ) );
  }
}
