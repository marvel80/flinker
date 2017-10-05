package me.flinker.more.samples;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class WikipediaDocAnalysis {

	public WikipediaDocAnalysis() {
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<WikipediaEditEvent> edit = env.addSource(new WikipediaEditsSource());

		KeyedStream<WikipediaEditEvent, String> keyEdits = edit.keyBy(value -> value.getUser());
		WindowedStream<WikipediaEditEvent, String, TimeWindow> keyedWindowStream = keyEdits.timeWindow(Time.seconds(3));
		DataStream<Tuple2<String, Long>> editsPerUser = getEditsPerUserViaAggregation(keyedWindowStream);

		// earlier used to just print
		editsPerUser.print();

		// now send to Kafka
		/*editsPerUser.map(new MapFunction<Tuple2<String, Long>, String>() {
			private static final long serialVersionUID = -3791157493663901971L;

			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				return value.toString();
			}
		}).addSink(new FlinkKafkaProducer08<>("localhost:9092", "test", new SimpleStringSchema()));*/

		env.execute();
	}

	private static DataStream<Tuple2<String, Long>> getEditsPerUserViaAggregation(
			WindowedStream<WikipediaEditEvent, String, TimeWindow> keyedStream) {

		return keyedStream
				.aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
					private static final long serialVersionUID = -4745753003313491676L;

					@Override
					public Tuple2<String, Long> createAccumulator() {
						return new Tuple2<String, Long>("", 0L);
					}

					@Override
					public void add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
						accumulator.f0 = value.getUser();
						accumulator.f1 += value.getByteDiff();
					}

					@Override
					public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
						return accumulator;
					}

					@Override
					public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
						a.f1 += b.f1;
						return a;
					}
				});
	}

	/*
	 * another way, using deprecated fold method.
	 */
	@SuppressWarnings({ "deprecation", "unused" })
	private static DataStream<Tuple2<String, Long>> getEditsPerUserViaFold(
			WindowedStream<WikipediaEditEvent, String, TimeWindow> keyedStream) {

		return keyedStream.fold(new Tuple2<String, Long>("", 0L),
				new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
					private static final long serialVersionUID = -179566653700890L;

					@Override
					public Tuple2<String, Long> fold(Tuple2<String, Long> accumulator, WikipediaEditEvent value)
							throws Exception {
						accumulator.f0 = value.getUser();
						accumulator.f1 += value.getByteDiff();
						return accumulator;
					}
				});
	}

	// TODO this is supposed to work same way as getEditsPerUserViaFold, but
	// some issue with Type extraction in lambdas
	@SuppressWarnings({ "deprecation", "unused" })
	private DataStream<Tuple2<String, Long>> getEditsPerUserViaFoldWithLambda(
			WindowedStream<WikipediaEditEvent, String, TimeWindow> keyedStream) {

		return keyedStream.fold(new Tuple2<String, Long>("", 0L),
				(Tuple2<String, Long> accumulator, WikipediaEditEvent value) -> {
					accumulator.f0 = value.getUser();
					accumulator.f1 += value.getByteDiff();
					return accumulator;
				}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
				}));
	}

}
