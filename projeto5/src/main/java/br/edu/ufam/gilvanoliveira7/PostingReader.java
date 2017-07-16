package br.edu.ufam.gilvanoliveira7;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;

public class PostingReader {

  public static PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>
    readPostings(BytesWritable bytesWritable) throws IOException{

    byte[] bytes = bytesWritable.getBytes();

    ByteArrayInputStream postingsByteStream = new ByteArrayInputStream(bytes);
    DataInputStream inStream = new DataInputStream(postingsByteStream);

    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

    int docno = 0;
    int nextDgap = 0;
    int termFreq = -1;
    int numPostings = WritableUtils.readVInt(inStream);


    for(int i = 0; i < numPostings; i++){

      nextDgap = WritableUtils.readVInt(inStream);
      termFreq = WritableUtils.readVInt(inStream);

      docno = docno + nextDgap;

      postings.add(new PairOfInts(docno, termFreq));
    }


    return new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(new IntWritable(numPostings), postings);

  }

}
