package cascading.json.operation;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertNotNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.junit.Test;

public class JSONFlattenTestCase {

    @Test
    public void testJSONFlatten() {
        Scheme sourceScheme = new TextLine(new Fields("line"));
        Tap input = new Hfs(sourceScheme, "data/input1.json");
        Pipe assembly = new Pipe("json_flatten");

        JSONSplitter jsonSplitter = new JSONSplitter(new Fields("name", "age", "phones"), "name", "age", "phones");
        assembly = new Each(assembly, new Fields("line"), jsonSplitter, new Fields("name", "age", "phones"));

        JSONFlatten jsonFlatten = new JSONFlatten(new Fields("phone_number", "phone_type"), "number", "type");
        assembly = new Each(assembly, new Fields("phones"), jsonFlatten, new Fields("name", "age", "phone_number", "phone_type"));

        assembly = new Each(assembly, AssertionLevel.STRICT, new AssertNotNull());

        Tap output = new Hfs(new TextLine(), "output/flatten", true);
        FlowConnector connector = new HadoopFlowConnector();
        Flow flow = connector.connect(input, output, assembly);
        flow.complete();
    }
}
