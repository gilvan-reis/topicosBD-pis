package br.edu.ufam.gilvanoliveira7;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

/**
* Hello world!
*
*/

/*public class BuildInvertedIndexCompressedByte extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static Map<String,Integer> inMapperHash;
        private static Text WORD;
        private static IntWritable IW;


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            inMapperHash = new HashMap<String,Integer>();
            WORD = new Text();
            IW = new IntWritable();
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

            String line = values.toString();

            //line normalization
            line = line.replaceAll("[\'\",.:;=$#@%\\*!\\?\\[\\]\\(\\)\\{\\}<>&]","");
            line = line.toLowerCase();
            //System.out.println("line: " + line);

            //line tokenization
            StringTokenizer tok = new StringTokenizer(line);
            String token = "";

            while (tok.hasMoreTokens()) {
                token = tok.nextToken();

                while((token.startsWith("'")) || (token.startsWith("-"))){
                    token = token.substring(1,token.length());
                }

                if(token.endsWith("-")){
                    token = token.substring(0,token.length()-1);
                }

                if(token.length() == 0){
                    continue;
                }

                Integer gotCount = inMapperHash.get(token);

                if (gotCount != null) {
                    gotCount++;
                } else {
                    gotCount = 1;
                }

                inMapperHash.put(token,gotCount);
                //System.out.println("token: " + token + "  cont: " + gotCount);
                //Thread.sleep(1000);
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : inMapperHash.keySet()) {
                String keyString = key + "," + "1";
                WORD.set(keyString);
                IW.set(inMapperHash.get(key));
                //System.out.println("key: " + key + " count final: " + inMapperHash.get(key));
                context.write(WORD,IW);
            }
        }

    }

    private static final class InvertedIndexPartitioner extends Partitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text key,IntWritable value,int numReducers) {
            String[] term_docid = key.toString().split(",");
            System.out.println("Im getting term:  ---- " + term_docid[0] + " ---- into the partitioner!");
            return Integer.parseInt(term_docid[0]) % numReducers;
        }

    }

    private static final class InvertedIndexReducer extends Reducer<Text,IntWritable,Text,BytesWritable> {
        private final static Text TERM = new Text();

        private final static ByteArrayOutputStream postingByteStream = new ByteArrayOutputStream();
        private final static DataOutputStream outStream = new DataOutputStream(postingByteStream);

        private static int lastDocno = 0;
        private static int thisDocno = 0;
        private static int dGapInt = 0;

        private static int docFreq = 0;
        private static int termFreq = 0;
        private static String currentTerm = null;
        private static String lastTerm = new String();

        @Override
        public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
            if (currentTerm != null && !lastTerm.equals(currentTerm.toString())) {
                TERM.set(currentTerm);
                outStream.flush();
                postingByteStream.flush();

                ByteArrayOutputStream toWrite = new ByteArrayOutputStream(4 + postingByteStream.size());
                DataOutputStream out = new DataOutputStream(toWrite);
                WritableUtils.writeVInt(out, docFreq);
                out.write(postingByteStream.toByteArray());

                context.write(TERM, new BytesWritable(toWrite.toByteArray()) );

                postingByteStream.reset();
                //Reset counters
                lastDocno = 0;
                docFreq = 0;
                String[] term_docid = key.toString().split(",");
                currentTerm = term_docid[0];
                lastTerm = currentTerm;

            }
            String[] term_docid = key.toString().split(",");
            currentTerm = term_docid[0];

            Iterator<IntWritable> iter = value.iterator();

            while (iter.hasNext()) {
                docFreq++;
                termFreq = iter.next().get();
                dGapInt = thisDocno - lastDocno;
                WritableUtils.writeVInt(outStream,dGapInt);
                WritableUtils.writeVInt(outStream,termFreq);

                lastDocno = thisDocno;
            }

            docFreq++;

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            TERM.set(currentTerm);
            outStream.flush();
            postingByteStream.flush();

            ByteArrayOutputStream toWrite = new ByteArrayOutputStream(4 + postingByteStream.size());
            DataOutputStream out = new DataOutputStream(toWrite);
            WritableUtils.writeVInt(out, docFreq);
            out.write(postingByteStream.toByteArray());

            context.write(TERM, new BytesWritable(toWrite.toByteArray()) );

            postingByteStream.close();
            outStream.close();
        }

    }*/

    /*private BuildInvertedIndexCompressed() {}

        @Override
        public int run(String[] argv) throws Exception {


            /*LOG.info("Tool: " + WordCount.class.getSimpleName());
            LOG.info(" - input path: " + args.input);
            LOG.info(" - output path: " + args.output);
            LOG.info(" - number of reducers: " + args.numReducers);
            LOG.info(" - use in-mapper combining: " + args.imc);*/

    /*        int numReducers = 1;

            Configuration conf = getConf();
            Job job = Job.getInstance(conf);
            job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
            job.setJarByClass(BuildInvertedIndexCompressed.class);

            //job.setNumReduceTasks(args.numReducers);

            FileInputFormat.setInputPaths(job, new Path(argv[0]));
            FileOutputFormat.setOutputPath(job, new Path(argv[1]));

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            //job.setOutputFormatClass(MapFileOutputFormat.class);

            //job.setMapperClass(args.imc ? MyMapperIMC.class : MyMapper.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setPartitionerClass(InvertedIndexPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);

            // Delete the output directory if it exists already.
            Path outputDir = new Path(argv[1]);
            FileSystem.get(conf).delete(outputDir, true);

            long startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

            return 0;
        }

        /**
        * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
        */
        /*public static void main(String[] args) throws Exception {
            ToolRunner.run(new BuildInvertedIndexCompressed(), args);
        }

    }*/
