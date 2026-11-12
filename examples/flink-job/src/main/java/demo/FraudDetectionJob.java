package demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/** Example job to try the Flink Pipeline tool window on: open this file with the plugin installed. */
public class FraudDetectionJob {

    static final OutputTag<Transaction> SUSPICIOUS = new OutputTag<>("suspicious") {};
    static final MapStateDescriptor<String, Double> RULES =
            new MapStateDescriptor<>("rules", Types.STRING, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .fromData(new Transaction("alice", 12.5, 1L), new Transaction("bob", 9800.0, 2L))
                .name("Transactions")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((t, ts) -> t.timestamp));

        DataStream<Rule> rules = env.fromData(new Rule("limit", 5000.0)).name("Rules");

        SingleOutputStreamOperator<Transaction> screened = transactions
                .process(new Screener()).name("Screen").uid("screen");

        BroadcastStream<Rule> broadcastRules = rules.broadcast(RULES);
        DataStream<Alert> alerts = screened
                .keyBy(t -> t.account)
                .connect(broadcastRules)
                .process(new RuleEvaluator()).name("Evaluate rules").setParallelism(4);

        totalsPerMinute(screened).print().name("Totals");

        alerts.print().name("Alerts");
        screened.getSideOutput(SUSPICIOUS).rebalance().print().name("Suspicious");

        env.execute("Fraud detection");
    }

    /** Helper method: the plugin follows it and shows its operators inline. */
    static DataStream<Transaction> totalsPerMinute(DataStream<Transaction> in) {
        return in
                .keyBy(t -> t.account)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .reduce((a, b) -> new Transaction(a.account, a.amount + b.amount, b.timestamp))
                .name("Total per minute");
    }

    static class Screener extends ProcessFunction<Transaction, Transaction> {
        @Override
        public void processElement(Transaction t, Context ctx, Collector<Transaction> out) {
            if (t.amount > 1000) ctx.output(SUSPICIOUS, t);
            out.collect(t);
        }
    }

    static class RuleEvaluator extends KeyedBroadcastProcessFunction<String, Transaction, Rule, Alert> {
        @Override
        public void processElement(Transaction t, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
            Double limit = ctx.getBroadcastState(RULES).get("limit");
            if (limit != null && t.amount > limit) out.collect(new Alert(t.account, t.amount));
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out) throws Exception {
            ctx.getBroadcastState(RULES).put(rule.name, rule.limit);
        }
    }

    public static class Transaction {
        public String account;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String account, double amount, long timestamp) {
            this.account = account;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    public static class Rule {
        public String name;
        public double limit;

        public Rule() {}

        public Rule(String name, double limit) {
            this.name = name;
            this.limit = limit;
        }
    }

    public static class Alert {
        public String account;
        public double amount;

        public Alert() {}

        public Alert(String account, double amount) {
            this.account = account;
            this.amount = amount;
        }
    }
}
