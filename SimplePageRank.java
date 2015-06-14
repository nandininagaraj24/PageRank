import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SimplePageRank {

	private static int num_iterations = 6;
	private static double pageRank;
	private static int totalNodes = 685230333;
    private static final long PRECISION = 10000;
    private static final double DAMPING_FACTOR = 0.85; 
    private static enum Residual{
    	RESIDUAL;
    }
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    
	   
	        
	    //value will be the adjacency list passed to mapper
	    //key will be the node id
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String valuesInside[] = line.split("#");

	    	//System.out.println("Inside Mapper");
	    	//System.out.println("Node id is: "+valuesInside[0] );
	    	//valuesInside has nodeid#pagerank#nodeid1,nodeid2,nodeid3 where nodeid1,nodeid2 and nodeid3 are
	    	//adjacency list of nodeid
	    	double nodePageRank = Double.parseDouble(valuesInside[1]);
	    	//System.out.println("Node's pagerank is: "+nodePageRank );
	    	if(valuesInside.length ==3){
	    		if(valuesInside[2].length() > 0){
	    			String adjList[] = valuesInside[2].split(",");

	    			long adjacencyListCount =adjList.length; 
	    	//		System.out.println("Adjacency list is: "+ valuesInside[2]);

	    			if(adjacencyListCount != 0 )
	    				pageRank = nodePageRank / (double)adjacencyListCount;
	    			else{
	    				pageRank = nodePageRank;
	    			}
	    	//		System.out.println("Node's new pagerank is: "+pageRank );
	    			Text mapKey = new Text(valuesInside[0]); //nodeid
	    			Text mapValues = new Text("IsNode" + "#" + valuesInside[1] +"#"+ valuesInside[2]); //pagerank#nodeid1,nodeid2
	    			context.write(mapKey, mapValues ); //emit nodeid and nodeDetails

	    			for(int i=0; i < adjList.length;i++){
	    				context.write(new Text(adjList[i]),new Text(Double.toString(pageRank))); //emit adjnode and pagerank
	    			}

	    		}
	    	}
	    	else{
	    		Text mapKey = new Text(valuesInside[0]); //nodeid
    			Text mapValues = new Text("IsNode" + "#" + valuesInside[1] +"#"+ ""); //pagerank#nodeid1,nodeid2
    			context.write(mapKey, mapValues ); //emit nodeid and nodeDetails

    			
	    	}
	    }
	}      
	 public static class Reduce extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	        
	        double newPageRank = 0.0;
	        double oldPageRank = 0.0;
	        double residualError = 0.0;
	        double pageRankSum = 0.0;
	        
	       // System.out.println("Inside Reducer");
	        
	        
	        String adjList = "";
	       String nodeId = null;
	        for (Text val : values) {
	            String isnode[] = val.toString().split("#"); //IsNode#pagerank#nodeid1,nodeid2
	            
	            //split based on delimiter # to distinguish between node and pagerank
	            if(isnode[0].equalsIgnoreCase("IsNode")){
	            	nodeId = key.toString();
	            	//System.out.println("NodeID is: "+ nodeId);
	            	if(isnode.length == 3){
	            		//adj.append(c)
	            		oldPageRank = Double.parseDouble(isnode[1]);
	            		if(isnode[2].length() > 1){
	            			adjList = isnode[2];
	            		}
	            	}
	            }
	            else{
	            	pageRankSum += Double.parseDouble(val.toString());
	            }
	        }
	     //   context.write(key, new Text(sum));
	        
	        newPageRank = (DAMPING_FACTOR *pageRankSum) + ((1.0-DAMPING_FACTOR)/totalNodes);
	        residualError = Math.abs((oldPageRank - newPageRank)/newPageRank);
	        long residualValue = (long)residualError*PRECISION;
	        context.getCounter(Residual.RESIDUAL).increment(residualValue);
	        
	        //emit node, and its details 
	        //details are: nodeid#pagerank#nodeid1,nodeid2
	        String residualOutput = key.toString() + "#" + newPageRank + "#" + adjList;
	        
	        //emit here:
	        context.write(key, new Text(residualOutput));
	    }
	 }


	

	
	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		
		int count =1;
		long sumResidualError = 0;
		double avgResidualError = 0.0;
		
		while(count <= num_iterations){
			Configuration conf = new Configuration();

			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "simplepagerank");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
		if(count == 1){
				FileInputFormat.addInputPath(job, new Path("Input"));
			}
		else{
				FileInputFormat.addInputPath(job, new Path("Output " + "temp" + count));
			}

			FileOutputFormat.setOutputPath(job, new Path("Output " + "temp" + (count+1)));
			
			job.waitForCompletion(true);

			sumResidualError = job.getCounters().findCounter(Residual.RESIDUAL).getValue();
			avgResidualError = (sumResidualError/PRECISION)/totalNodes;
			job.getCounters().findCounter(Residual.RESIDUAL).setValue(0);
			
			count++;

		}

		if(avgResidualError < 0.001){
			System.out.println("Average residual error is: "+ avgResidualError);
			
		}
		System.out.println("Average residual error is: "+ avgResidualError);
		System.out.println("Simple Page Rank didn't converge after "+ num_iterations + " iterations.");
	}

}
