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
        env.setParallelism(4);

        DataStream<Batch> source = env.addSource(new ChallengerSource());

        // Run to take metrics without optimisation
        System.out.println("Start Pipeline without optimization");
        execute_computation(env, source, false);

        // Run to take metrics with optimisation
        System.out.println("Start Pipeline with optimization");
        execute_computation(env, source, true);

    }

}
