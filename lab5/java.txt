import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
  // Make sure to filter characters and convert to lower case.
        word.set(itr.nextToken().replaceAll("[^a-zA-Z- ]", "").toLowerCase());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
  
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // mapper for sorting
  public static class FrequencySortMapper extends  Mapper<Object, Text, IntWritable, Text>
  {
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException
    {
      String line = value.toString();
      StringTokenizer stringTokenizer = new StringTokenizer(line);
      {
        
        String[] splitted = line.split("\\s+");

        int frequency = Integer.parseInt(splitted.get(1)); 
        String word = splitted.get(0);

        context.write(new IntWritable(frequency), new Text(word));
      }
    }
  }

  // reducer class for sorting
  public static class FrequencySortReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        result.set(key);
        context.write(new Text(val), key);
      }
    }
  }

  public static class IntComparator extends WritableComparator {
    public IntComparator() {
      super(IntWritable.class);
    }

    private Integer int1;
    private Integer int2;

    @Override
    public int compare(byte[] raw1, int offset1, int length1, byte[] raw2,
        int offset2, int length2) {
      int1 = ByteBuffer.wrap(raw1, offset1, length1).getInt();
      int2 = ByteBuffer.wrap(raw2, offset2, length2).getInt();

      return int2.compareTo(int1);
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    // Add the second map reduce job
    Configuration conf_sort = new Configuration();
    Job job_sort = Job.getInstance(conf_sort, "frequency sort");
    job_sort.setJarByClass(WordCount.class);
    job_sort.setMapperClass(FrequencySortMapper.class);
    job_sort.setCombinerClass(FrequencySortReducer.class);
    job_sort.setReducerClass(FrequencySortReducer.class);
    job_sort.setOutputKeyClass(Text.class);
    job_sort.setOutputValueClass(IntWritable.class);
    job_sort.setSortComparatorClass(IntComparator.class);
    FileInputFormat.addInputPath(job_sort, new Path(args[1]));
    FileOutputFormat.setOutputPath(job_sort, new Path(args[2]));
    System.exit(job_sort.waitForCompletion(true) ? 0 : 1);
  }
}
