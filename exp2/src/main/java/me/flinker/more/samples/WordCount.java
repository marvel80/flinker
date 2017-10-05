package me.flinker.more.samples;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * </ul>
 *
 */
public class WordCount {
	private static final String SAMPLE_TEXT = "demo demo demo of dataset API";

	public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> data = env.fromElements(SAMPLE_TEXT);

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<String, Integer>> trasnsformedData = data
				/*
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1069293713093807126L;

					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						String[] tokens = value.trim().toLowerCase().split("\\W+");
						for (String token : tokens) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}
				})*/
		
				//functionally,same as above but fancy. why not ?? its JAVA8 :P
				.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
					List<String> tokens = Arrays.asList(value.trim().toLowerCase().split("\\W+"));
					tokens.forEach(token -> out.collect(new Tuple2<String, Integer>(token, 1)));
				})
				.returns(TypeInformation
						// works with both : TupleTypeInfo or TypeHint
						//new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
						.of( new TypeHint<Tuple2<String, Integer>>() {}))
				.groupBy(0).sum(1);
		
		try {
			trasnsformedData.print();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
