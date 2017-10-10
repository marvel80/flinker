package me.flinker.more.samples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * demo for iteration on integer. each iteration (10) adds 1 to existing partial result.
 *
 */
public class IntegerAddOneIteration {
	public static void main(String[] args) throws Exception{
		//Obtain an execution environment,
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Load/create the initial data,
		DataSet<Integer> input = env.fromElements(0);
		
		//start iteration
		IterativeDataSet<Integer> startOfIteration = input.iterate(10);
		
		//Specify transformations on this data
		DataSet<Integer> toBeFedback = startOfIteration.map(i -> i+1);
		
		//close iteration
		DataSet<Integer> count = startOfIteration.closeWith(toBeFedback);
		
		//Specify where to put the results of your computations,
		//print takes care of trigger execution as well
		count.print();
		
		//Trigger the program execution
		
	}

}
