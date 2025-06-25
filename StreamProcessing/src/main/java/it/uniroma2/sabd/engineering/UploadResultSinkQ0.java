package it.uniroma2.sabd.engineering;


import it.uniroma2.sabd.engineering.ChallengerSource;
import it.uniroma2.sabd.model.Batch;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashSet;
import java.util.Set;

public class UploadResultSinkQ0 implements SinkFunction<Batch> {
    private final Set<Long> uploaded = new HashSet<>();

    @Override
    public void invoke(Batch batch, Context context) {
        if (ChallengerSource.BENCH_ID != null && !uploaded.contains(batch.batch_id)) {
            ChallengerSource.uploadResultQ0(batch, ChallengerSource.BENCH_ID);
            uploaded.add((long) batch.batch_id);
        }
    }
}