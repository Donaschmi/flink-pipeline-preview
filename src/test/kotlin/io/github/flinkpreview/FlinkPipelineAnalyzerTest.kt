package io.github.flinkpreview

import com.intellij.psi.PsiJavaFile
import com.intellij.testFramework.fixtures.LightJavaCodeInsightFixtureTestCase
import io.github.flinkpreview.analysis.FlinkPipelineAnalyzer

/**
 * Runs the analyzer on small jobs written against stub versions of the Flink classes
 * (same packages and method names as the real API, simplified signatures).
 */
class FlinkPipelineAnalyzerTest : LightJavaCodeInsightFixtureTestCase() {

    override fun setUp() {
        super.setUp()
        FLINK_STUBS.forEach { myFixture.addClass(it) }
    }

    fun testLinearChainWithKeyedWindow() {
        val dump = analyze(
            """
            import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
            import org.apache.flink.streaming.api.datastream.*;
            import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
            import java.time.Duration;

            public class Job {
                public static void main(String[] args) throws Exception {
                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                    DataStream<String> lines = env.fromSource(new KafkaSource(), null, "Kafka lines");
                    DataStream<Integer> lengths = lines
                        .filter(s -> !s.isEmpty()).name("Drop empty")
                        .map(String::length);
                    lengths.keyBy(x -> x % 2)
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                        .reduce((a, b) -> a + b).uid("sum").setParallelism(4)
                        .sinkTo(new KafkaSink()).name("Out");
                    env.execute("job");
                }
                static class KafkaSource {}
                static class KafkaSink {}
            }
            """
        )
        assertEquals(
            """
            SOURCE fromSource(KafkaSource, null, …) name=Kafka lines
            OPERATOR filter(λ) name=Drop empty
            OPERATOR map(String::length)
            OPERATOR reduce(λ) tags=[window(TumblingEventTimeWindows(10s))] uid=sum p=4
            SINK sinkTo(KafkaSink) name=Out
            Kafka lines -> Drop empty
            Drop empty -> map(String::length)
            map(String::length) -> reduce(λ) PARTITION [keyBy(λ)]
            reduce(λ) -> Out
            """.trimIndent(),
            dump,
        )
    }

    fun testVariablesSideOutputsUnionConnectAndHelpers() {
        val dump = analyze(
            """
            import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
            import org.apache.flink.streaming.api.datastream.*;
            import org.apache.flink.util.OutputTag;

            public class Job {
                static final OutputTag<String> LATE = new OutputTag<>("late");

                public static void main(String[] args) throws Exception {
                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                    DataStream<String> a = env.fromElements("x");
                    DataStream<String> b = env.fromElements("y");
                    SingleOutputStreamOperator<String> main = a.process(new Splitter());
                    DataStream<String> late = main.getSideOutput(LATE);
                    DataStream<String> merged = main.union(b);
                    merged = enrich(merged);
                    merged.connect(late).process(new Joiner()).print();
                    audit(late);
                }

                static DataStream<String> enrich(DataStream<String> in) {
                    return in.map(s -> s + "!").name("Enrich");
                }

                static void audit(DataStream<String> s) {
                    s.rebalance().sinkTo(null).name("Audit");
                }

                static class Splitter {}
                static class Joiner {}
            }
            """
        )
        assertEquals(
            """
            SOURCE fromElements("x")
            SOURCE fromElements("y")
            OPERATOR process(Splitter)
            UNION union()
            OPERATOR map(λ) name=Enrich
            OPERATOR process(Joiner) tags=[connect]
            SINK print()
            SINK sinkTo(null) name=Audit
            fromElements("x") -> process(Splitter)
            process(Splitter) -> union()
            fromElements("y") -> union()
            union() -> Enrich
            Enrich -> process(Joiner)
            process(Splitter) -> process(Joiner) SIDE_OUTPUT [side output LATE]
            process(Joiner) -> print()
            process(Splitter) -> Audit SIDE_OUTPUT [side output LATE, rebalance]
            """.trimIndent(),
            dump,
        )
    }

    fun testCaretInHelperShowsTheCallingJob() {
        val file = myFixture.configureByText(
            "Job.java",
            """
            import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
            import org.apache.flink.streaming.api.datastream.*;

            public class Job {
                public static void main(String[] args) {
                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                    clean(env.fromElements(1)).print();
                }
                static DataStream<Integer> clean(DataStream<Integer> in) {
                    return in.filter(x -> x > 0<caret>);
                }
            }
            """.trimIndent(),
        ) as PsiJavaFile
        val result = FlinkPipelineAnalyzer.analyze(file, myFixture.caretOffset)
        assertEquals("Job.main()", result.scope)
        assertEquals(
            """
            SOURCE fromElements(1)
            OPERATOR filter(λ)
            SINK print()
            fromElements(1) -> filter(λ)
            filter(λ) -> print()
            """.trimIndent(),
            result.graph!!.dump().trim(),
        )
    }

    fun testDataStreamV2Api() {
        val dump = analyze(
            """
            import org.apache.flink.datastream.api.ExecutionEnvironment;
            import org.apache.flink.datastream.api.stream.*;

            public class Job {
                public static void main(String[] args) throws Exception {
                    ExecutionEnvironment env = ExecutionEnvironment.getInstance();
                    NonKeyedPartitionStream<String> words = env.fromSource(null, "words");
                    words.keyBy(w -> w)
                        .process(new Counter()).withName("Count")
                        .toSink(null);
                }
                static class Counter {}
            }
            """
        )
        assertEquals(
            """
            SOURCE fromSource(null, "words") name=words
            OPERATOR process(Counter) name=Count
            SINK toSink(null)
            words -> Count PARTITION [keyBy(λ)]
            Count -> toSink(null)
            """.trimIndent(),
            dump,
        )
    }

    fun testFileWithoutPipeline() {
        val file = myFixture.configureByText("Plain.java", "public class Plain { void f() { System.out.println(1); } }") as PsiJavaFile
        val result = FlinkPipelineAnalyzer.analyze(file, null)
        assertNull(result.graph)
        assertNotNull(result.message)
    }

    private fun analyze(code: String): String {
        val file = myFixture.configureByText("Job.java", code.trimIndent()) as PsiJavaFile
        val result = FlinkPipelineAnalyzer.analyze(file, null)
        assertNotNull(result.message ?: "", result.graph)
        return result.graph!!.dump().trim()
    }

    companion object {
        private val FLINK_STUBS = listOf(
            """
            package org.apache.flink.util;
            public class OutputTag<T> { public OutputTag(String id) {} }
            """,
            """
            package org.apache.flink.api.common.functions;
            public interface MapFunction<T, O> { O map(T value) throws Exception; }
            """,
            """
            package org.apache.flink.api.common.functions;
            public interface FilterFunction<T> { boolean filter(T value) throws Exception; }
            """,
            """
            package org.apache.flink.api.common.functions;
            public interface ReduceFunction<T> { T reduce(T a, T b) throws Exception; }
            """,
            """
            package org.apache.flink.api.java.functions;
            public interface KeySelector<IN, KEY> { KEY getKey(IN value) throws Exception; }
            """,
            """
            package org.apache.flink.streaming.api.windowing.assigners;
            public class TumblingEventTimeWindows { public static TumblingEventTimeWindows of(java.time.Duration size) { return null; } }
            """,
            """
            package org.apache.flink.streaming.api.environment;
            import org.apache.flink.streaming.api.datastream.DataStreamSource;
            public class StreamExecutionEnvironment {
                public static StreamExecutionEnvironment getExecutionEnvironment() { return null; }
                public <T> DataStreamSource<T> fromSource(Object source, Object watermarks, String name) { return null; }
                @SafeVarargs public final <T> DataStreamSource<T> fromElements(T... data) { return null; }
                public Object execute(String name) throws Exception { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            import org.apache.flink.api.common.functions.*;
            import org.apache.flink.api.java.functions.KeySelector;
            public class DataStream<T> {
                public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> f) { return null; }
                public SingleOutputStreamOperator<T> filter(FilterFunction<T> f) { return null; }
                public <R> SingleOutputStreamOperator<R> process(Object f) { return null; }
                public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) { return null; }
                @SafeVarargs public final DataStream<T> union(DataStream<T>... others) { return null; }
                public <R> ConnectedStreams<T, R> connect(DataStream<R> other) { return null; }
                public DataStream<T> rebalance() { return null; }
                public DataStreamSink<T> sinkTo(Object sink) { return null; }
                public DataStreamSink<T> print() { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            public class SingleOutputStreamOperator<T> extends DataStream<T> {
                public SingleOutputStreamOperator<T> name(String name) { return this; }
                public SingleOutputStreamOperator<T> uid(String uid) { return this; }
                public SingleOutputStreamOperator<T> setParallelism(int p) { return this; }
                public <X> SideOutputDataStream<X> getSideOutput(org.apache.flink.util.OutputTag<X> tag) { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {}
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            public class SideOutputDataStream<T> extends DataStream<T> {}
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            import org.apache.flink.api.common.functions.ReduceFunction;
            public class KeyedStream<T, K> extends DataStream<T> {
                public WindowedStream<T, K, Object> window(Object assigner) { return null; }
                public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> f) { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            import org.apache.flink.api.common.functions.ReduceFunction;
            public class WindowedStream<T, K, W> {
                public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> f) { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            public class ConnectedStreams<A, B> {
                public <R> SingleOutputStreamOperator<R> process(Object f) { return null; }
            }
            """,
            """
            package org.apache.flink.streaming.api.datastream;
            public class DataStreamSink<T> {
                public DataStreamSink<T> name(String name) { return this; }
            }
            """,
            // DataStream V2 API (Flink 2.x)
            """
            package org.apache.flink.datastream.api;
            import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
            public class ExecutionEnvironment {
                public static ExecutionEnvironment getInstance() { return null; }
                public <T> NonKeyedPartitionStream<T> fromSource(Object source, String name) { return null; }
            }
            """,
            """
            package org.apache.flink.datastream.api.stream;
            import org.apache.flink.api.java.functions.KeySelector;
            public interface NonKeyedPartitionStream<T> {
                <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> key);
                <R> ProcessConfigurableAndNonKeyedPartitionStream<R> process(Object f);
                void toSink(Object sink);
            }
            """,
            """
            package org.apache.flink.datastream.api.stream;
            public interface KeyedPartitionStream<K, T> {
                <R> ProcessConfigurableAndNonKeyedPartitionStream<R> process(Object f);
            }
            """,
            """
            package org.apache.flink.datastream.api.stream;
            public interface ProcessConfigurableAndNonKeyedPartitionStream<T> extends NonKeyedPartitionStream<T> {
                ProcessConfigurableAndNonKeyedPartitionStream<T> withName(String name);
            }
            """,
        ).map { it.trimIndent() }
    }
}
