/**
* Código desenvolvido por: Gilvan Oliveira.
*/

package br.edu.ufam.gilvanoliveira7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BooleanRetrievalHbase extends Configured implements Tool {
  private HTableInterface table;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;

  private BooleanRetrievalHbase() {}

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayList<PairOfInts> fetchPostings(String term) throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    Result result = table.get(get);

    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> postings = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    try {
      DataInputStream data = new DataInputStream(new ByteArrayInputStream(result.getValue(BuildInvertedIndexHbase.CF, BuildInvertedIndexHbase.FREQ)));
      postings.readFields(data);
    }catch(Exception e){
      e.printStackTrace();
    }
    return postings.getRightElement();
  }


  private String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    return reader.readLine();
  }

  private static final String TABLE = "table";
  private static final String WORD = "word";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("table").hasArg().withDescription("HBase table name").create(TABLE));
    options.addOption(OptionBuilder.withArgName("word").hasArg().withDescription("word to look up").create(WORD));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(TABLE) || !cmdline.hasOption(WORD)) {
        System.out.println("args: " + Arrays.toString(args));
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(this.getClass().getName(), options);
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    String tableName = cmdline.getOptionValue(TABLE);
    String word = cmdline.getOptionValue(WORD);

    Configuration conf = getConf();
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
    table = hbaseConnection.getTable(tableName);

    String[] queries = { "outrageous fortune AND", "white rose AND", "means deceit AND",
        "white red OR rose AND pluck AND", "unhappy outrageous OR good your AND OR fortune AND" };

    for (String q : queries) {
      System.out.println("Query: " + q);

      runQuery(q);
      System.out.println("");
    }

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalHbase(), args);
  }
}
