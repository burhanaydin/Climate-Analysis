package climateAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class CustomFileInputFormat extends TextInputFormat{

	public RecordReader<LongWritable, Text> myCustomLineRecordReader;
	@Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        this.myCustomLineRecordReader = (RecordReader<LongWritable, Text>) new MyCustomLineRecordReader();
    	return  myCustomLineRecordReader;
    }

}