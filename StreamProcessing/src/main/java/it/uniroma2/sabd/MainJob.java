package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.*;
import it.uniroma2.sabd.model.Batch;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static it.uniroma2.sabd.controller.Controller.execute_computation;

public class MainJob {

    public static void main(String[] args) throws Exception {

        System.out.println("Start Pipeline: Q1 - Saturation + Q2 - Outlier Detection + Q3 - Clustering");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Batch> source = env.addSource(new ChallengerSource());

        // Run to take metrics without optimisation and without collecting metrics for comparison with Kafka Streams
        execute_computation(env, source, false, false);

        // Run to take metrics with optimisation and without collecting metrics for comparison with Kafka Streams
        execute_computation(env, source, false, true);

        // Run to take metrics without optimisation and collect metrics for comparison with Kafka Streams
        execute_computation(env, source, true, false);

    }

}
