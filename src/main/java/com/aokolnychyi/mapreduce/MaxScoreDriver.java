package com.aokolnychyi.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxScoreDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (!isInputValid(args)) return -1;

        final Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "Max score");
        job.setJarByClass(MaxScoreDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxScoreMapper.class);
        job.setCombinerClass(MaxScoreReducer.class);
        job.setReducerClass(MaxScoreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final MaxScoreDriver maxScoreDriver = new MaxScoreDriver();
        int exitCode = ToolRunner.run(maxScoreDriver, args);
        System.exit(exitCode);
    }

    private boolean isInputValid(String[] args) {
        final boolean isInputValid;
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            isInputValid = false;
        } else {
            isInputValid = true;
        }
        return isInputValid;
    }
}
