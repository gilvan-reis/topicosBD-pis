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
* Código desenvolvido por: Ariel Afonso, Felipe Manzoni e Gilvan Oliveira.
*
*/

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class InvertedIndexMapper extends Mapper<LongWritable, Text, TextIntWritablePairComparable, IntWritable> {

        private static Map<String,Integer> inMapperHash;
        private static Text WORD;
        private static IntWritable IW;
        private static final TextIntWritablePairComparable pair = new TextIntWritablePairComparable();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            inMapperHash = new HashMap<String,Integer>();
            WORD = new Text();
            IW = new IntWritable();
        }

        @Override
        public void map(LongWritable docno, Text values, Context context) throws IOException, InterruptedException {

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

                //inMapperHash.put(token + "," + (int) docno.get(), gotCount);
                //inMapperHash.put(token + "," + "1",gotCount);
                if(inMapperHash.containsKey(token+",1")){
                    inMapperHash.put(token + "," + "1", inMapperHash.get(token+",1") + gotCount);
                }else{
                    inMapperHash.put(token + "," + "1", gotCount);
                }

                //System.out.println("token: " + token + "  cont: " + gotCount);
                //Thread.sleep(1000);
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : inMapperHash.keySet()) {
                String[] term_docid = key.split(",");
                pair.set(new Text(term_docid[0]), new IntWritable(Integer.valueOf(term_docid[1])) );
                //String keyString =  + "," + term_docid[1];
                //WORD.set(keyString);
                IW.set(inMapperHash.get(key));
                //DOCNO.set();
                //System.out.println("key: " + key + " count final: " + inMapperHash.get(key));
                context.write(pair,IW);
            }
        }

    }

    private static final class InvertedIndexPartitioner extends Partitioner<TextIntWritablePairComparable,IntWritable> {

        @Override
        public int getPartition(TextIntWritablePairComparable key,IntWritable value,int numReducers) {

            //String[] term_docid = key.toString().split(",");
            //System.out.println("Im getting term:  ---- " + term_docid[0] + " ---- into the partitioner!");
            return Integer.valueOf(key.getLeftElement().toString()) % numReducers;
        }

    }

    private static final class InvertedIndexReducer extends Reducer<TextIntWritablePairComparable,IntWritable,Text,ArrayListWritable<PairOfWritables<IntWritable,VIntWritable>>> {//BytesWritable> {
        private final static Text TERM = new Text();

        private static final ArrayListWritable<PairOfWritables<IntWritable,VIntWritable>> index_postings = new ArrayListWritable<PairOfWritables<IntWritable,VIntWritable>>();
        private static PairOfWritables<IntWritable,VIntWritable> postings;
        //private static VIntWritable leftInt, rightInt;
        private static IntWritable leftInt;
        private static VIntWritable rightInt;


        private static int lastDocno = 0;
        private static int thisDocno = 0;
        private static int dGapInt = 0;

        private static int docFreq = 0;
        private static int termFreq = 0;
        private static String currentTerm = null;
        private static String lastTerm = new String();
        private static String[] term_docid;

        @Override
        public void reduce(TextIntWritablePairComparable key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
            currentTerm = key.getLeftElement().toString();
            thisDocno = key.getRightElement().get();

            if (currentTerm != null && !lastTerm.equals(currentTerm)) {
                TERM.set(currentTerm);

                context.write(TERM,index_postings);
                //System.out.println("current: " + currentTerm + " lastTerm: " + lastTerm);

                index_postings.clear();

                //Reset counters
                lastDocno = 0;
                docFreq = 0;
                termFreq = 0;
                currentTerm = key.getLeftElement().toString();
                thisDocno = key.getRightElement().get();
                lastTerm = currentTerm;

            }

            Iterator<IntWritable> iter = value.iterator();

            while (iter.hasNext()) {
                termFreq += iter.next().get();
            }
            docFreq++;
            dGapInt = thisDocno - lastDocno;
            lastDocno = thisDocno;
            //leftInt = new VIntWritable(dGapInt);
            //leftInt = new IntWritable(dGapInt);
            leftInt = new IntWritable(thisDocno);
            rightInt = new VIntWritable(termFreq);
            postings = new PairOfWritables<IntWritable,VIntWritable>();
            postings.set(leftInt,rightInt);
            index_postings.add(postings);
            //termFreq=0;
            docFreq++;

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            TERM.set(currentTerm);
            context.write(TERM,index_postings);
            index_postings.clear();

        }

    }

    private BuildInvertedIndexCompressed() {}

        @Override
        public int run(String[] argv) throws Exception {


            /*LOG.info("Tool: " + WordCount.class.getSimpleName());
            LOG.info(" - input path: " + args.input);
            LOG.info(" - output path: " + args.output);
            LOG.info(" - number of reducers: " + args.numReducers);
            LOG.info(" - use in-mapper combining: " + args.imc);*/

            int numReducers = 1;

            Configuration conf = getConf();
            Job job = Job.getInstance(conf);
            job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
            job.setJarByClass(BuildInvertedIndexCompressed.class);

            //job.setNumReduceTasks(args.numReducers);

            FileInputFormat.setInputPaths(job, new Path(argv[0]));
            FileOutputFormat.setOutputPath(job, new Path(argv[1]));

            job.setMapOutputKeyClass(TextIntWritablePairComparable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(ArrayListWritable.class);

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
        public static void main(String[] args) throws Exception {
            ToolRunner.run(new BuildInvertedIndexCompressed(), args);
        }

    }
