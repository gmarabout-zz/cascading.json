package cascading.json.operation.aggregator;

import java.util.Iterator;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import cascading.flow.FlowProcess;
import cascading.json.JSONUtils;
import cascading.json.JSONWritable;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.TupleEntry;

/**
 * JSONTupleAggregator: 
 *    groups input tuples into a json string. e.g.
 *
 * {@code}
 *    input:
 *    (a,          <- group
 *      (foo, bar),
 *      (bam, baz))
 * 
 *    emits:
 *    (a, '["foo","bar"],["foo","baz"]')
 * 
 * {code}
 *
 * @author <a href="mailto:nate@natemurray.com">Nate Murray</a>
 */

public class JSONTupleAggregator extends BaseOperation<JSONTupleAggregator.Context> implements Aggregator<JSONTupleAggregator.Context>
  {
  /** Class Context is used to hold intermediate values. */
  protected static class Context
    {
    JSONArray json;

    public Context() 
      {
        this.reset();
      }

    public Context reset()
      {
      this.json = new JSONArray();
      return this;
      }
    }

  /**
   * Constructs a new instance that returns the average of the values encoutered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  public JSONTupleAggregator( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    if( aggregatorCall.getContext() != null )
      aggregatorCall.getContext().reset();
    else
      aggregatorCall.setContext( new Context() );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
      Context context = aggregatorCall.getContext();
      TupleEntry arguments = aggregatorCall.getArguments();
      Tuple tuple = arguments.getTuple();
      JSONArray tupie = new JSONArray();
      for ( int i=0; i< tuple.size(); i++ ) {
        tupie.element( tuple.getString( i ) );
      }
      context.json.element( JSONArray.toCollection( tupie ) );
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  private Tuple getResult( AggregatorCall<Context> aggregatorCall )
    {
    Context context = aggregatorCall.getContext();
    return new Tuple( context.json.toString() );
    }
  }
