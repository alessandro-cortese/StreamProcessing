package it.uniroma2.sabd.query;

//import it.uniroma2.sabd.model.Batch;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//import java.util.Map;
//
//public class Q2SlidingWindowProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, Batch, Batch> {
//    @Override
//    public void processElement(Batch batch, Context ctx, Collector<Batch> out) {
//        // Stub: crea outliers finti
//        batch.q2_outliers = Map.of(
//                "P1", new int[]{10, 20},
//                "\u03b4P1", "6001"
//        );
//        out.collect(batch);
//    }
//}