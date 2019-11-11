package bd;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class RTTIMethod extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two argument is expected.");
        }
        return new GenericUDAFMin();
    }

    public static class GenericUDAFMin extends GenericUDAFEvaluator{

        private PrimitiveObjectInspector inputOI;


        static class TimeAndIdAgg implements AggregationBuffer {

            String time;

            String id;

            @Override
            public String toString() {
                return id;
            }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            inputOI = (PrimitiveObjectInspector) parameters[0];
            super.init(m, parameters);
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }


        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new TimeAndIdAgg();
        }

        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            TimeAndIdAgg agg = (TimeAndIdAgg)aggregationBuffer;
            agg.id = null;
            agg.time =null;
        }

        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            String time = PrimitiveObjectInspectorUtils.getString(objects[0], inputOI);
            String id =  PrimitiveObjectInspectorUtils.getString(objects[1], inputOI);
            TimeAndIdAgg agg = (TimeAndIdAgg)aggregationBuffer;
            if(agg.time ==null || agg.time.compareTo(time)==1){
                agg.time = time;
                agg.id= id;
            }
        }

        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            TimeAndIdAgg agg = (TimeAndIdAgg)aggregationBuffer;
            return new Text(agg.time+","+agg.id);
        }

        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            TimeAndIdAgg agg = (TimeAndIdAgg)aggregationBuffer;
            String[] agg2 = PrimitiveObjectInspectorUtils.getString(o, inputOI).split(",");
            String time = agg2[0];
            String id = agg2[1];
            if(agg.time ==null || agg.time.compareTo(time)==1){
                agg.time = time;
                agg.id = id;
            }
        }

        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            TimeAndIdAgg agg = (TimeAndIdAgg)aggregationBuffer;
            return new Text(agg.id);
        }
    }
}
