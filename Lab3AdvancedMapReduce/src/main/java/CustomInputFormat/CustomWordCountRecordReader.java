package CustomInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomWordCountRecordReader extends RecordReader<Text, IntWritable> {
	
	private FileSplit fileSplit;
	private Configuration conf;
	private IntWritable value = new IntWritable();
	private Text key = new Text();
	private boolean processed = false;

	@Override
	public void close() throws IOException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) arg0;
		this.conf = arg1.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			key.set(file.toString());
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			String fileContents = new String(contents, "UTF-8");
			
			String goalString = conf.get("stringToSearch");
			int index = fileContents.indexOf(goalString);
			int count = 0;
			while (index != -1) {
			    count++;
			    fileContents = fileContents.substring(index + 1);
			    index = fileContents.indexOf(goalString);
			}
			value.set(count);
			processed = true;
			return true;
		}
		return false;
	}

}
