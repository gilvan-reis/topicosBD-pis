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

import java.io.FileInputStream;
import java.io.LineNumberReader;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
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


public class StripesPMISemCombiner extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(StripesPMISemCombiner.class);

  //classes do primeiro job para contar os tokens
	private static class TokensCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static Text TOKEN = new Text();
		private final static IntWritable UM = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString(); 
			//limpar caracteres especiais
			line = line.replaceAll("[\",.:;=$#@%\\*!\\?\\[\\]\\(\\)\\{\\}<>&]","");
			line = line.toLowerCase();

			StringTokenizer t = new StringTokenizer(line);
			Set<String> unique = new HashSet<String>();
			String token = "";

			while(t.hasMoreTokens()){
				token = t.nextToken();

				//limpa lixo dos tokens
				while((token.startsWith("'")) || (token.startsWith("-"))){
					token = token.substring(1,token.length());
				}
				if(token.endsWith("-")){
					token = token.substring(0,token.length()-1);
				}

				if(token.length() == 0){
					continue;
				}

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
	private static class StripesPMIMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {

		private static final HMapStIW mapStrInt = new HMapStIW();
		private static final Text chave = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			//limpar caracteres especiais
			line = line.replaceAll("[\",.:;=$#@%\\*!\\?\\[\\]\\(\\)\\{\\}&<>]","");
			line = line.toLowerCase();

			String token;
			StringTokenizer t = new StringTokenizer(line);

			Set<String> sortedTerms = new TreeSet<String>();
			while(t.hasMoreTokens()){
				token = t.nextToken();
				//limpa lixo dos tokens
				while((token.startsWith("'")) || (token.startsWith("-"))){
					token = token.substring(1,token.length());
				}
				if(token.endsWith("-")){
					token = token.substring(0,token.length()-1);
				}

				if(token.length() == 0){
					continue;
				}
				sortedTerms.add(token); //remove tokens duplicados e ordena
			}

			String left = "";
			String right = "";

			String[] terms = new String[sortedTerms.size()]; 
			sortedTerms.toArray(terms);

			//para cada par de termos, emita os pares e 1
			for(int leftTermIndex = 0; leftTermIndex < terms.length; leftTermIndex++){
				left = terms[leftTermIndex];

				for(int rightTermIndex = leftTermIndex + 1; rightTermIndex < terms.length; rightTermIndex++) {
					right = terms[rightTermIndex];

					if(!mapStrInt.containsKey(right)){
						mapStrInt.put(right, 1);
					}
				}

				chave.set(left);
				context.write(chave,mapStrInt);
				mapStrInt.clear();

			}
		}
	}

	private static class StripesPMIReducer extends Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {

		private static Map<String, Integer> tokensCount = new HashMap<String, Integer>();
		private static Map<String, Integer> mapTotal = new HashMap<String, Integer>();
		private static PairOfStrings PAIR = new PairOfStrings();

		private static DoubleWritable PMI = new DoubleWritable();
		private static double totalDocs = 0.0; 

		@Override
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);

			Path inFile = new Path(conf.get("intermediatePath") + "/part-r-00000");

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

			///--- contabiliza linhas do arquivo de entrada
			LineNumberReader lineCounter = new LineNumberReader(new InputStreamReader(new FileInputStream(conf.get("inputPath"))));
			String nextLine = null;
			try {
				while ((nextLine = lineCounter.readLine()) != null) {
					if (nextLine == null)
						break;
				}
				totalDocs = lineCounter.getLineNumber() * 1.0;
				LOG.info("Numero total de linha do arquivo: " + totalDocs);
			} catch (Exception done) {
				done.printStackTrace();
			}

		}

		@Override
		public void reduce(Text key, Iterable<HMapStIW> values, Context context ) throws IOException, InterruptedException{

			for(HMapStIW value: values){
				for(String chave: value.keySet()){
					if(mapTotal.containsKey(chave)){
						mapTotal.put(chave, mapTotal.get(chave) + value.get(chave));
					}else{
						mapTotal.put(chave, value.get(chave));
					}
				}
			}

			String left = key.toString();


			for (String chave:mapTotal.keySet()) {
				PAIR.set(left,chave);
				double probPair = mapTotal.get(chave) / totalDocs;
				double probLeft = tokensCount.get(left) / totalDocs;
				double probRight = tokensCount.get(chave) / totalDocs;

				double pmi = Math.log10(probPair / (probLeft * probRight));
				PMI.set(pmi);
				context.write(PAIR,PMI);
			}

			mapTotal.clear();
		}
	}

	public StripesPMISemCombiner() {}

	private static final class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		String input;

		@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
		String output;

		@Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
		int numReducers = 1;
	}

	@SuppressWarnings("static-access")
	public int run(String[] argv) throws Exception {
		final Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		String inputPath = args.input;
		String outputPath = args.output;
		String intermediatePath = "./tokens_count_file";

		int reduceTasks = args.numReducers;

		LOG.info("Tool: " + StripesPMISemCombiner.class.getSimpleName() + " TokensCount Part");
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + intermediatePath);
		LOG.info(" - number of reducers: " + reduceTasks);

		Configuration conf = getConf();
		conf.set("intermediatePath", intermediatePath);
		conf.set("inputPath",inputPath);

		Job job1 = Job.getInstance(conf);
		job1.setJobName(StripesPMISemCombiner.class.getSimpleName() + " TokensCount");
		job1.setJarByClass(StripesPMISemCombiner.class);

		job1.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setMapperClass(TokensCountMapper.class);
		job1.setCombinerClass(TokensCountReducer.class);
		job1.setReducerClass(TokensCountReducer.class);	

		// exclui o diretorio antigo, caso exista
		Path intermediateDir = new Path(intermediatePath);
		FileSystem.get(conf).delete(intermediateDir, true);

		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


		// inicia o segundo job
		LOG.info("------------------------------------------------------------------------------\n");
		LOG.info("Tool: " + StripesPMISemCombiner.class.getSimpleName() + " Stripes Part");
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		Job job2 = Job.getInstance(conf);
		job2.setJobName(StripesPMISemCombiner.class.getSimpleName() + " StripesPMICalcuation");
		job2.setJarByClass(StripesPMISemCombiner.class);

		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2,  new Path(inputPath));
		TextOutputFormat.setOutputPath(job2, new Path(outputPath));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(HMapStIW.class);
		job2.setMapperClass(StripesPMIMapper.class);
		job2.setReducerClass(StripesPMIReducer.class);

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
		ToolRunner.run(new StripesPMISemCombiner(), args);
	}
}