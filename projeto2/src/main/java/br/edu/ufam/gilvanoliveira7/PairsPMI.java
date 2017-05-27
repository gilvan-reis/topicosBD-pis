//codigo base retirado de cimbriano/MapReduce-assignments

package br.edu.ufam.icomp.gilvanoliveira7;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class PairsPMI extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  //classes do primeiro job para contar os tokens
  private static class TokensCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Text TOKEN = new Text();
    private final static IntWritable UM = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

      String line = value.toString(); 
      StringTokenizer t = new StringTokenizer(line);
      Set<String> unique = new HashSet<String>();
      String token = "";

      while(t.hasMoreTokens()){
        token = t.nextToken();

        if(unique.add(token)){ //se o token ainda foi lido na linha, emite o token e 1
          TOKEN.set(token);
          context.write(TOKEN, UM);
        }
      }
    }
  }

  private static class TokensCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private final static IntWritable SOMA = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

      int soma = 0;
      for(IntWritable value : values){
        soma += value.get();
      }

      SOMA.set(soma);
      context.write(key, SOMA);
    }
  }


  //------------------------------------------------------------
  //Classes do segundo job que vai efetivamente calcular o PMI
  private static class PairsPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    private final static PairOfStrings PAR = new PairOfStrings();
    private static Map<PairOfStrings, Integer> inMapperHash;

    @Override
    public void setup(Context context){
    	inMapperHash = new HashMap<PairOfStrings, Integer>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

      String line = value.toString();
      StringTokenizer t = new StringTokenizer(line);

      Set<String> sortedTerms = new TreeSet<String>();
      while(t.hasMoreTokens()){
        sortedTerms.add(t.nextToken()); //remove tokens duplicados e ordena
      }

      String left = "";
      String right = "";

      String[] terms = new String[sortedTerms.size()]; 
      sortedTerms.toArray(terms);

      //para cada par de termos, emita os pares e 1
      for(int leftTermIndex = 0; leftTermIndex < terms.length; leftTermIndex++){
        for(int rightTermIndex = leftTermIndex + 1; rightTermIndex < terms.length; rightTermIndex++) {
          left = terms[leftTermIndex];
          right = terms[rightTermIndex];

          PAR.set(left, right);
          inMapperHash.put(PAR, inMapperHash.get(PAR).intValue()+1);
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Map.Entry<PairOfStrings, Integer>> it = inMapperHash.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<PairOfStrings, Integer> entry = it.next();
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
    }
  }

  private static class PairsPMIReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
    
    private static Map<String, Integer> tokensCount = new HashMap<String, Integer>();
    
    private static DoubleWritable PMI = new DoubleWritable();
    private static double totalDocs = 156215.0;
    
    @Override
    public void setup(Context context) throws IOException{
      //TODO Read from intermediate output of first job
      // and build in-memory map of terms to their individual totals
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      
//      Path inFile = new Path(conf.get("intermediatePath"));
      Path inFile = new Path("/Users/chris/Projects/UMD/MapReduce-assignments/assignment2/appearance_totals_pairs/part-r-00000");

      if(!fs.exists(inFile)){
        throw new IOException("File Not Found: " + inFile.toString());
      }
      
      BufferedReader reader = null;
      try{
        FSDataInputStream in = fs.open(inFile);
        InputStreamReader inStream = new InputStreamReader(in);
        reader = new BufferedReader(inStream);
        
      } catch(FileNotFoundException e){
        throw new IOException("Exception thrown when trying to open file.");
      }
      
      
      String line = reader.readLine();
      while(line != null){
        
        String[] parts = line.split("\\s+");
        if(parts.length != 2){
          LOG.info("Input line did not have exactly 2 tokens: '" + line + "'");
        } else {
          tokensCount.put(parts[0], Integer.parseInt(parts[1]));
        }
        line = reader.readLine();
      }
      
      reader.close();
      
    }
    
    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
      
      int pairSum = 0;
      for(IntWritable value : values) {
        pairSum += value.get();
      }
      
      if(pairSum >= 10){ //apenas calcula se o pair tiver mais de 10 ocorrencias

        String left = pair.getLeftElement();
        String right = pair.getRightElement();

        double probPair = pairSum / totalDocs;
        double probLeft = tokensCount.get(left) / totalDocs;
        double probRight = tokensCount.get(right) / totalDocs;

        double pmi = Math.log(probPair / (probLeft * probRight));


        pair.set(left, right);

        PMI.set(pmi);
        context.write(pair, PMI);
      }

    }

  }

  public PairsPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try{
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if(!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + args.toString());
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    
    //TODO This output path is for the 2nd job's.
    //    The fits job will have an intermediate output path from which the second job's reducer will read
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String intermediatePath = "/Users/chris/Projects/UMD/MapReduce-assignments/assignment2/appearance_totals_pairs";
    
    
    
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? 
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + PairsPMI.class.getSimpleName() + " Appearances Part");
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + intermediatePath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        conf.set("intermediatePath", intermediatePath);
        
        Job job1 = Job.getInstance(conf);
        job1.setJobName(PairsPMI.class.getSimpleName() + " AppearanceCount");
        job1.setJarByClass(PairsPMI.class);

        job1.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(TokensCountMapper.class);
        job1.setCombinerClass(TokensCountReducer.class);
        job1.setReducerClass(TokensCountReducer.class);

        // Delete the output directory if it exists already.
        Path intermediateDir = new Path(intermediatePath);
        FileSystem.get(conf).delete(intermediateDir, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        
        // Start second job
        
        LOG.info("Tool: " + PairsPMI.class.getSimpleName() + " Pairs Part");
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);
        
        Job job2 = Job.getInstance(conf);
        job2.setJobName(PairsPMI.class.getSimpleName() + " PairsPMICalcuation");
        job2.setJarByClass(PairsPMI.class);
        
        job2.setNumReduceTasks(reduceTasks);
        
        FileInputFormat.setInputPaths(job2,  new Path(inputPath));
        TextOutputFormat.setOutputPath(job2, new Path(outputPath));
        
        //TODO Which output key??
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapperClass(PairsPMIMapper.class);
        job2.setReducerClass(PairsPMIReducer.class);
        
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        
        
        return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    ToolRunner.run(new PairsPMI(), args);
  }
}