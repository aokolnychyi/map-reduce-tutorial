package com.aokolnychyi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    enum CounterTypes {
        MALFORMED_SCORE
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String record = value.toString();
        final boolean isRecordValid = ScoreParser.isRecordValid(record);

        if (isRecordValid) {
            final String playerName = ScoreParser.getPlayerName(record);
            final int score = ScoreParser.getScore(record);
            context.write(new Text(playerName), new IntWritable(score));
        } else {
            System.err.println("Ignoring corrupted input: " + record);
            final Counter corruptedRecordCounter = context.getCounter(CounterTypes.MALFORMED_SCORE);
            corruptedRecordCounter.increment(1);
        }
    }
}
