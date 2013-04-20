package cascading.json.operation;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import junit.framework.TestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class JSONParserTestCase extends TestCase {
	public void testJSONParsing() {
		Scheme sourceScheme = new TextLine( new Fields( "line" ) );
		Tap input = new Hfs( sourceScheme, "data/input1.json" );
		Pipe assembly = new Pipe( "json_parser" );
		
		JSONParser jsonParser = new JSONParser(new Fields("json"));
		assembly = new Each(assembly, new Fields("line"), jsonParser);
		
		assembly = new Each(assembly, AssertionLevel.STRICT, new AssertSizeEquals(1));
		
		// Branch 1
		JSONSplitter splitter1 = new JSONSplitter(new Fields("name", "address"), "name", "address");
		Pipe branch1 = new Pipe("branch1", assembly);
		branch1 = new Each(branch1, new Fields("json"), splitter1, new Fields("name", "address"));
		branch1 = new Each(branch1, AssertionLevel.STRICT, new AssertSizeEquals(2));
		branch1 = new Each(branch1, AssertionLevel.STRICT, new AssertNotNull());
		
		// Branch 2
		JSONSplitter splitter2 = new JSONSplitter(new Fields("name", "phones"), "name", "phones");
		Pipe branch2 = new Pipe("branch2", assembly);
		branch2 = new Each(branch2,  new Fields("json"), splitter2, new Fields("name", "phones"));
		branch2 = new Each(branch2, AssertionLevel.STRICT, new AssertSizeEquals(2));
		branch2 = new Each(branch2, AssertionLevel.STRICT, new AssertNotNull());
		
		// CoGroup the branches:
		Pipe grouped = new CoGroup(branch1, new Fields("name"), branch2, new Fields("name"), new Fields("name", "address", "name2", "phones"));
		
		Tap output = new Hfs(new TextLine(), "output/parser", true);
		FlowConnector connector = new HadoopFlowConnector();
		Flow flow = connector.connect(input, output, grouped);
		
		flow.complete();
	}
}
