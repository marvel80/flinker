package me.flinker.more.samples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class WordCountOnPort {

	private static final String BROKER = "localhost:9092"; 
	private static final String TOPIC = "test";
	
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.socketTextStream("localhost", 12345, "\n");

		DataStream<WordWithCount> counts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
			private static final long serialVersionUID = 4462228192298221793L;

			public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
				for (String word : s.split("\\s")) {
					collector.collect(new WordWithCount(word, 1));
				}
			}
		}).keyBy("word").timeWindow(Time.seconds(3)).reduce(new ReduceFunction<WordWithCount>() {
			private static final long serialVersionUID = -8230111537126273540L;

			public WordWithCount reduce(WordWithCount wc1, WordWithCount wc2) throws Exception {
				return new WordWithCount(wc1.word, wc1.count + wc2.count);
			}
		});
		
		//counts.print().setParallelism(1);
		
		// send to Kafka topic --> test
		counts
		.map(value-> value.toString())
		/*.map(new MapFunction<WordCountOnPort.WordWithCount, String>() {
			private static final long serialVersionUID = 5073431879636192442L;

			@Override
			public String map(WordWithCount value) throws Exception {
				return value.toString();
			}
		})*/
		.addSink(new FlinkKafkaProducer08<String>(BROKER, TOPIC, new SimpleStringSchema()));
		
		try {
			env.execute("wordcount");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class WordWithCount {
		private String word;
		private int count;
	}

}
