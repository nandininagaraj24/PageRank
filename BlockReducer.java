/* 
 * Should take input as

 * 1. set of vertices in that block
 * 2. current page ranks of all those nodes
 * 3. outgoing edges of all the nodes in the block
 * 4. boundary edges and their pageranks
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;



/*
 * Output should be updated pagerank
 * and residuals
 */

public class BlockReducer {


	public static HashMap<String, Double>PR  = new HashMap<>();
	public static HashMap<String, Double>NPR  = new HashMap<>();
	public static HashMap<String, ArrayList<String>>BE = new HashMap<>();
	public static HashMap<String, Double>BC = new HashMap<String, Double>();
	public static HashMap<String, Node>NodeDetails = new HashMap<>();
	//public static double oldPageRank = 0.0;
	public static ArrayList<String>Vertices = new ArrayList<>();
	



	public static Double JacobiIterateBlockOnce(){

		Node getNode;
		Double newPageRank = 0.0;
		Double oldPR = 0.0;
		Double pageRankBoundary =0.0;
		Double residualError =0.0;
		ArrayList<String>temp = new ArrayList<String>();
		for(int i =0; i< Vertices.size(); i++)
			NPR.put(Vertices.get(i), 0.0);
		
		for(int i = 0; i< Vertices.size(); i++){
			//NPR.put(Vertices.get(i), 0.0);
			oldPR = PR.get(Vertices.get(i));

			if(BE.containsKey(Vertices.get(i))){

				
				temp=BE.get(Vertices.get(i));

				for(int j =0; j < temp.size(); j++){
					getNode = NodeDetails.get(temp.get(j));
					newPageRank += getNode.getNodePageRank()/getNode.getDegree();
				}
			}

			if(BC.containsKey(Vertices.get(i))){
				pageRankBoundary = BC.get(Vertices.get(i));
				newPageRank += pageRankBoundary;
			}

			newPageRank = BlockDriver.DAMPING_FACTOR*newPageRank +(double) (1-BlockDriver.DAMPING_FACTOR)/BlockDriver.totalNodes;
			NPR.put(Vertices.get(i),newPageRank);
			
			residualError += Math.abs((oldPR- newPageRank))/(newPageRank);
		}
		
		for(int i = 0; i < Vertices.size(); i++){
		
			Double nodenewPageRank = NPR.get(Vertices.get(i));
			PR.put(Vertices.get(i), nodenewPageRank);
		}
	
		Double avgresidualError = residualError/Vertices.size();
		return avgresidualError;
	}

	
	public static Double Gauss_SeidelIterateBlockOnce(){

		Node getNode;
		Double newPageRank = 0.0;
		Double oldPR = 0.0;
		Double pageRankBoundary =0.0;
		Double residualError =0.0;
		
		for(int i = 0; i< Vertices.size(); i++){
			//NPR.put(Vertices.get(i), 0.0);
			oldPR = PR.get(Vertices.get(i));

			if(BE.containsKey(Vertices.get(i))){

				ArrayList<String>temp = new ArrayList<String>();
				temp=BE.get(Vertices.get(i));

				for(int j =0; j < temp.size(); j++){
					getNode = NodeDetails.get(temp.get(j));
					newPageRank += getNode.getNodePageRank()/getNode.getDegree();
				}
			}

			if(BC.containsKey(Vertices.get(i))){
				pageRankBoundary = BC.get(Vertices.get(i));
				newPageRank += pageRankBoundary;
			}

			newPageRank = BlockDriver.DAMPING_FACTOR*newPageRank + (1-BlockDriver.DAMPING_FACTOR)/BlockDriver.totalNodes;
			PR.put(Vertices.get(i),newPageRank);
			residualError += Math.abs((oldPR- newPageRank)/(newPageRank));
		}
		
		Double avgresidualError = residualError/Vertices.size();
		return avgresidualError;
	
	}
	
	
public static class Reduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {


		ArrayList<String>blockEdge = new ArrayList<>();
		Double boundaryCondition = 0.0;
		Double inBlockResidualError =0.0;
		Node getnode;
		Double residualOverEachNode = 0.0;
		Double avgResidualOverEachNode = 0.0;
		Text reducerKey;
		Text reducerValues;
		BE.clear();
		BC.clear();
		PR.clear();
		NodeDetails.clear();
		Vertices.clear();
		for(Text val : values){

			String reducerInputValues[] = val.toString().split("\\s+");
			String nodeID="";
			Double oldpageRankForThisNode;
			//In case of emit PageRank PR: 
			//Populate PR
			//INPUT : PR NODEID PAGERANK ADJACENCYLIST
			//KEY :BLOCKID
			//it is this node's oldPR

			if(reducerInputValues[0].equalsIgnoreCase("PR")){
				Node node = new Node();

				nodeID = reducerInputValues[1];
				oldpageRankForThisNode = Double.parseDouble(reducerInputValues[2]);
				node.setNodeId(nodeID);
				node.setNodePageRank(oldpageRankForThisNode);
				if(reducerInputValues.length == 4){

					
					node.setAdjList(reducerInputValues[3]);
					node.setDegree(reducerInputValues[3].split(",").length);
				}

				else{

					node.setAdjList("");
					node.setDegree(0);
				}

				Vertices.add(nodeID);
				PR.put(nodeID, oldpageRankForThisNode);
				NodeDetails.put(nodeID, node);

			}
			else if(reducerInputValues[0].equalsIgnoreCase("BE")){

				//in block edge
				String destnodeid = reducerInputValues[2];
				if(BE.containsKey(destnodeid))
					blockEdge = BE.get(destnodeid);

				blockEdge.add(reducerInputValues[1]);
				BE.put(reducerInputValues[2], blockEdge);

			}
			else if(reducerInputValues[0].equals("BC")){

				//it is boundary condition
				if(BC.containsKey(reducerInputValues[2]))
					boundaryCondition = BC.get(reducerInputValues[2]);

				boundaryCondition += Double.parseDouble(reducerInputValues[3]);
				BC.put(reducerInputValues[2], boundaryCondition);
			}

		}

		int count =0;
		do{

			inBlockResidualError = JacobiIterateBlockOnce();
			count++;

		}while(count <=7 && inBlockResidualError > 0.001);

		
		for(int  i =0 ; i< Vertices.size(); i++){
			getnode = NodeDetails.get(Vertices.get(i));
			residualOverEachNode += Math.abs(getnode.getNodePageRank() - PR.get(Vertices.get(i)))/PR.get(Vertices.get(i));
		}
		
		avgResidualOverEachNode = residualOverEachNode/Vertices.size();
		long passResidualToCounter = (long)(avgResidualOverEachNode*BlockDriver.PRECISION);
		context.getCounter(BlockDriver.Residual.RESIDUAL).increment(passResidualToCounter);
		
		
		//emit back output:
		for(int i= 0; i < Vertices.size(); i++){
			getnode = NodeDetails.get(Vertices.get(i));
			reducerValues = new Text(PR.get(Vertices.get(i))+" "+getnode.getAdjList());
			reducerKey =new Text( Vertices.get(i));
			context.write(reducerKey, reducerValues);
		}
		
	}
}


}

