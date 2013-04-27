package cascading.json.operation;

import net.sf.json.JSONObject;
import org.junit.Test;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;


/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONOperationTestCase {

    private static String JSON_DATA = "{ name: \"Grégoire\", data: { age: 33, address: { city: \"Paris\", street:\"Sibuet\"} }}";

    @Test
    public void testDefaultJSONPathResolver() {
        JSONPathResolver resolver = new JSONOperation.DefaultJSONPathResolver(":");
        JSONObject jsonObject = JSONObject.fromObject(JSON_DATA);
        assertEquals("Paris", resolver.resolve(jsonObject, "data:address:city"));
        assertEquals("Grégoire", resolver.resolve(jsonObject, "name"));
        assertNull(resolver.resolve(jsonObject, "toto:titi:tata"));
        assertNull(resolver.resolve(jsonObject, "data2:address:zip"));
    }

}
