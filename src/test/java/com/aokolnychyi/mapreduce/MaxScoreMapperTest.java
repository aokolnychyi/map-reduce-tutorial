package com.aokolnychyi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

import static com.aokolnychyi.mapreduce.MaxScoreMapper.CounterTypes.MALFORMED_SCORE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MaxScoreMapperTest {

    private MaxScoreMapper maxScoreMapper = new MaxScoreMapper();
    private MaxScoreReducer maxScoreReducer = new MaxScoreReducer();
    //    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
//    private MapReduceDriver mapReduceDriver = MapReduceDriver.newMapReduceDriver(maxScoreMapper, maxScoreReducer);

    @Test
    public void processValidRecord() throws IOException {
        final Text inputText = new Text("Player1 14");
        final Text outputKey = new Text("Player1");
        final IntWritable outputValue = new IntWritable(14);

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(maxScoreMapper)
                .withInput(new LongWritable(0), inputText)
                .withOutput(outputKey, outputValue)
                .runTest();
    }

    @Test
    public void processValidRecord2() throws IOException {
        final Text inputText = new Text("Player1 14");
        final Text inputText2 = new Text("Player1 24");
        final Text outputKey = new Text("Player1");
        final IntWritable outputValue = new IntWritable(24);

        new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>()
                .withMapper(maxScoreMapper)
                .withReducer(maxScoreReducer)
                .withInput(new LongWritable(), inputText)
                .withInput(new LongWritable(), inputText2)
                .withOutput(outputKey, outputValue)
                .runTest();
    }

    @Test
    public void processMissingScore() throws IOException {
        final Text inputText = new Text("Player1");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(maxScoreMapper)
                .withInput(new LongWritable(0), inputText)
                .runTest();
    }

    @Test
    public void processInvalidScore() throws IOException {
        final Text inputText = new Text("Player1 invalidScore");
        final Counters counters = new Counters();

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(maxScoreMapper)
                .withInput(new LongWritable(0), inputText)
                .withCounters(counters)
                .runTest();

        final Counter malformedScoreCounter = counters.findCounter(MALFORMED_SCORE);
        assertThat(malformedScoreCounter.getValue(), is(1L));
    }
}
