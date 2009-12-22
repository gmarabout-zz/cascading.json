package cascading.json.operation.aggregator;


import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.AssertionLevel;
import cascading.operation.Buffer;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import java.util.Iterator;
import junit.framework.TestCase;
import org.junit.* ;
import static org.junit.Assert.* ;


public class JSONTupleAggregatorTestCase extends CascadingTestCase {
  public JSONTupleAggregatorTestCase()
    {
    super("json tuple aggregator test case");
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

   @Test
   public void test_group_tuple_aggregator() {
     Tuple[] arguments = new Tuple[]{
       new Tuple("foo","bar"),
       new Tuple("foo","baz"),
       new Tuple("bing","bam")};

     Aggregator aggregator = new JSONTupleAggregator( new Fields("group") );

     Fields resultFields = new Fields("group");
     TupleListCollector resultEntryCollector = invokeAggregator(aggregator, arguments, resultFields);
     Iterator<Tuple> iterator = resultEntryCollector.iterator();

     Tuple t;
     String msg = "JSONTupleAggregator test failed";

     assertEquals(msg, 1, resultEntryCollector.size());

     t = iterator.next(); 
     assertEquals(msg, "[[\"foo\",\"bar\"],[\"foo\",\"baz\"],[\"bing\",\"bam\"]]", t.getString(0));
   }

   @Test
   public void test_group_tuple_aggregator_others() {
     Tuple[] arguments = new Tuple[]{
       new Tuple("zoo","sed"),
       new Tuple("zed","soo"),
       new Tuple("bff",null),
       new Tuple(null,3)};

     Aggregator aggregator = new JSONTupleAggregator( new Fields("group") );

     Fields resultFields = new Fields("group");
     TupleListCollector resultEntryCollector = invokeAggregator(aggregator, arguments, resultFields);
     Iterator<Tuple> iterator = resultEntryCollector.iterator();

     Tuple t;
     String msg = "JSONTupleAggregator test failed";

     assertEquals(msg, 1, resultEntryCollector.size());

     t = iterator.next(); 
     assertEquals(msg, "[[\"zoo\",\"sed\"],[\"zed\",\"soo\"],[\"bff\",\"\"],[\"\",\"3\"]]", t.getString(0));
   }

}
