import java.io.FileInputStream;
import java.io.LineNumberReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Estatisticas{

	public static void contabilizaPares(String path){
		try {
			LineNumberReader lineCounter = new LineNumberReader(new InputStreamReader(new FileInputStream(path)));
			String nextLine = null;

		
			while ((nextLine = lineCounter.readLine()) != null) {
				if (nextLine == null)
					break;
			}

			System.out.println("Numero total de linha do arquivo: " + lineCounter.getLineNumber());
		} catch (Exception done) {
			done.printStackTrace();
		}
	}

	public static void maiorPar(String path){
		String par;
		Double pmi=0.0;
		String maiorPar="";
		Double maiorPmi=0.0;

		try{
			BufferedReader reader = null;
			try{
				FileInputStream fis = new FileInputStream(path);
				InputStreamReader inStream = new InputStreamReader(fis);
				reader = new BufferedReader(inStream);

			} catch(FileNotFoundException e){
				throw new IOException("Exception thrown when trying to open file.");
			}


			String line = reader.readLine();
			while(line != null){

				String[] parts = line.split("\t+");
				
				par = parts[0];
				pmi = Double.parseDouble(parts[1]);
				//System.out.println(par+":"+pmi);

				if(pmi > maiorPmi){
					maiorPmi = pmi;
					maiorPar = par;
				}
				
				line = reader.readLine();
			}

			System.out.println(maiorPar+":"+maiorPmi);

			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void tresMaioresPmi(String path, String chave){
		java.util.List<java.util.Map.Entry<String,Double>> pairList= new java.util.ArrayList<>();
		Map.Entry<String,Double> pair;
		String par,esquerda;
		Double pmi=0.0;

		try{
			BufferedReader reader = null;
			try{
				FileInputStream fis = new FileInputStream(path);
				InputStreamReader inStream = new InputStreamReader(fis);
				reader = new BufferedReader(inStream);

			} catch(FileNotFoundException e){
				throw new IOException("Exception thrown when trying to open file.");
			}


			String line = reader.readLine();
			while(line != null){

				String[] parts = line.split("\t+");
				
				par = parts[0];
				pmi = Double.parseDouble(parts[1]);
				esquerda = par.split(",")[0];
				esquerda = esquerda.substring(1,esquerda.length());


				if(esquerda.equals(chave)){
					//System.out.println(par+":"+pmi);
					pair = new AbstractMap.SimpleEntry<>(par,pmi);
					pairList.add(pair);
				}
				
				line = reader.readLine();
			}

			Collections.sort(pairList, new Comparator() {
	            public int compare(Object o1, Object o2) {
	                Map.Entry<String,Double> e1 = (Map.Entry<String,Double>) o1;
	                Map.Entry<String,Double> e2 = (Map.Entry<String,Double>) o2;
	                return e1.getValue() < e2.getValue() ? -1 : (e1.getValue() > e2.getValue() ? +1 : 0);
	            }
	        });

	        for(Map.Entry<String,Double> entrada : pairList){
	        	System.out.println(entrada.getKey()+":"+entrada.getValue());
	        }	

			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}



	public static void main(String[] args) {
		//contabilizaPares("pmi/part-r-00000");
		//maiorPar("pmi/part-r-00000");
		//tresMaioresPmi("pmi/part-r-00000","life");
		tresMaioresPmi("pmi/part-r-00000","love");
	}
}