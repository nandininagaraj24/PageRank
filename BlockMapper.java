import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 */

/**
 * @author karteeka
 *
 */

/*0
 * 0 0 0.9 1,2,3
 * 
 * 0
 * 
 * 
 */
public class BlockMapper {


	private static double contributionPageRank = 0.0;


	public static class Map extends Mapper<LongWritable, Text, Text, Text> {



		//key will be block_id
		//value will be block_id node_id pagerank adjacencylist


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String Values = value.toString();
			Values.trim();

			//VALUE is of form NODEID PAGERANK ADJACENCYLIST
			String valuesInside[] = Values.split("\\s+");

			Text mapperValue;
			Text mapperKey;

			String NodeID = valuesInside[0];
			String PageRank = valuesInside[1];
			String adjList ="";
			if(valuesInside.length==3)
				adjList = valuesInside[2];

			int BlockID = find_block(Integer.parseInt(NodeID));
			//int BlockID = findBlockID(Integer.parseInt(NodeID));
			mapperKey = new Text(Integer.toString(BlockID));  //send Key as BlockID
			mapperValue = new Text("PR" + " "+NodeID+" "+ PageRank+ " "+ adjList); //emit value PR NodeID PageRank AdjList
			context.write(mapperKey, mapperValue);


			//if outgoing edges exist for the incoming node
			if( valuesInside.length ==3){
				//valuesInside has blockid nodeid pagerank nodeid1,nodeid2,nodeid3 where nodeid1,nodeid2 and nodeid3 are
				//adjacency list of nodeid
				double nodePageRank = Double.parseDouble(PageRank);
				String adj[] = adjList.split(",");
				int degree = adj.length;
				contributionPageRank = nodePageRank/degree;
				int blockIDOfAdjNode =0;


				for(int i=0; i< adj.length ;i++){

					//find block id of destination nodeid
					blockIDOfAdjNode = find_block(Integer.parseInt(adj[i]));
					//blockIDOfAdjNode = findBlockID(Integer.parseInt(adj[i]));
					mapperKey = new Text(Integer.toString(blockIDOfAdjNode));

					//now check if block ID of adj node is same as my blockID
					if(BlockID != blockIDOfAdjNode){
						//Block IDs are not same: So it is a boundary edge
						mapperValue = new Text("BC"+ " "+ NodeID+ " " + adj[i] + " "+ contributionPageRank);



					}else{

						mapperValue = new Text("BE" + " " + NodeID + " " + adj[i] );
					}
					context.write(mapperKey, mapperValue);

				}


			}
		}

		public static int find_block(int node){
			
			int BlocksPartition[] = { 10328, 10045, 10256, 10016,  9817, 10379,  9750,  9527, 10379, 10004, 10066, 10378, 10054,  9575, 10379, 10379,  9822, 10360, 10111, 10379, 10379, 10379,  9831, 10285, 10060, 10211, 10061, 10263,  9782,  9788, 10327, 10152, 10361,  9780,  9982, 10284, 10307, 10318, 10375,  9783,  9905, 10130,  9960,  9782,  9796, 10113,  9798,  9854,  9918,  9784, 10379, 10379, 10199, 10379, 10379, 10379, 10379, 10379,  9981,  9782,  9781, 10300,  9792,  9782,  9782,  9862,  9782,  9782};
			
			for(int  i=0; i < BlocksPartition.length;i++){
				
				if(node < BlocksPartition[i]){
					return BlocksPartition[i];
					
				}
			}
			return 0;
			
			
		}
		}
}


