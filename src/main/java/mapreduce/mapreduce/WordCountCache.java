package mapreduce.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCache extends Configured implements Tool{
	
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public List<String> wordList = new ArrayList<String>();
        private static final Pattern split = Pattern.compile("\\s*\\b\\s*");

        @Override
        public void setup(Context context)throws IOException, InterruptedException {
        	if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}

			URI[] paths = context.getCacheFiles();
        	String fileName = paths[0].toString();
            try {
            	BufferedReader reader = new BufferedReader(new FileReader(fileName));
                while(reader.readLine() != null) {
                    String line = reader.readLine();
                    for (String word : split.split(line)) {
						wordList.add(word);
					}
       }
                reader.close();
            } catch (FileNotFoundException e) {
    			System.err.println("Failed to open cached file: " + e);
    		} catch (IOException e) {
    			System.err.println("Failed to read cache file: " + e);
    		}
            super.setup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if(wordList.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "cache_word_count");
    	
    	if(args.length != 3) {
    		System.out.println("At least three parameters are requested.");
            System.exit(2);
    	}
    	
    	job.setJarByClass(WordCountCache.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new WordCountCache(), args);
		System.exit(res);
    }
}
