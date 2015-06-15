# PageRank
Simple and Blocked page rank computation
This project computes PageRank for a reasonably large Web graph (685230 nodes, 7600595 edges) with a residual error below 0.1%.
Each Reducer key corresponds to a single node of the graph. A Reduce task basically just updates the PageRank value for its node based on the PageRank values of the node’s immediate neighbors. The PageRank value is computed as it “flows” along a path through the graph requires a number of MapReduce passes proportional to the length of the path.
Better convergence by partitioning the Web graph into Blocks, and letting each Reduce task operate on an entire Block at once, propagating data along multiple edges within the Block. The idea is that each Reduce task loads its entire Block into memory and does multiple in-memory PageRank iterations on the Block, possibly even iterating until the Block has converged. When the Reduce task completes, it emits at least the updated PageRank values for every node of the Block.



