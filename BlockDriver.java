import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





/**
 * 
 */

/**
 * @author karteeka
 *
 */
public class BlockDriver {

	/**
	 * @param args
	 */
	public static int num_iterations = 6;
	public static double pageRank;
	public static int totalNodes = 685230;
    public static final long PRECISION = 10000;
    public static final double DAMPING_FACTOR = 0.85; 
    public static enum Residual{
    	RESIDUAL;
    }
	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		
		int count =1;
		long sumResidualError = 0;
		double avgResidualError = 0.0;
		
		
		//BlockParse.nodeBlockMapping();
		
		
		
		do{
			Configuration conf = new Configuration();

			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "blockdriver");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(BlockMapper.Map.class);
			job.setReducerClass(BlockReducer.Reduce.class);

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
			avgResidualError = ((double)sumResidualError/PRECISION)/68;
			job.getCounters().findCounter(Residual.RESIDUAL).setValue(0);
			
			System.out.println("Residual Error for iteration "+count + ": "+avgResidualError);
			
			count++;

		}while( avgResidualError > 0.001);

		if(avgResidualError < 0.001){
			System.out.println("Average residual error is: "+ avgResidualError);
			System.out.println("Blocked PR converged in:" + (count-1) + "iterations");
			
		}
		System.out.println("Average residual error is: "+ avgResidualError);
		System.out.println("Blocked Page Rank didn't converge after "+ num_iterations + " iterations.");
	}


	
	

}
