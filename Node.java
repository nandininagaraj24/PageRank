/**
 * 
 */

/**
 * @author karteeka
 *
 */
public class Node {

	String nodeId="";
	String adjList = "";
	Double nodePageRank = 0.0;
	int degree = 0;
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public String getAdjList() {
		return adjList;
	}
	public void setAdjList(String adjList) {
		this.adjList = adjList;
	}
	public Double getNodePageRank() {
		return nodePageRank;
	}
	public void setNodePageRank(Double nodePageRank) {
		this.nodePageRank = nodePageRank;
	}
	public int getDegree() {
		return degree;
	}
	public void setDegree(int degree) {
		this.degree = degree;
	}
	
	
	
}
