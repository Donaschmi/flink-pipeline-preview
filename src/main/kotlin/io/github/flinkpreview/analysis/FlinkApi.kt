package io.github.flinkpreview.analysis

import com.intellij.psi.PsiClass
import com.intellij.psi.PsiType
import com.intellij.psi.util.PsiUtil

/**
 * Knowledge about the Flink DataStream APIs: the DataStream API of Flink 1.x and 2.x, and the
 * DataStream V2 API of Flink 2.x. Classes are recognised by package and methods by name, so the
 * same rules work across Flink versions without depending on a particular Flink jar.
 */
object FlinkApi {

    private val STREAM_PACKAGES = listOf(
        "org.apache.flink.streaming.api.datastream.",   // DataStream API (1.x and 2.x)
        "org.apache.flink.datastream.api.stream.",      // DataStream V2 API (2.x)
    )

    private val API_PACKAGES = STREAM_PACKAGES + listOf(
        "org.apache.flink.streaming.api.environment.",
        "org.apache.flink.datastream.api.",
    )

    /** Classes in the stream packages that are utilities or config holders, not streams. */
    private val NOT_STREAMS = listOf(
        "org.apache.flink.streaming.api.datastream.AsyncDataStream",
        "org.apache.flink.streaming.api.datastream.DataStreamUtils",
        "org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes",
    )

    private const val DATA_STREAM_SINK = "org.apache.flink.streaming.api.datastream.DataStreamSink"

    /** Calls that configure the operator they are called on instead of adding one. */
    val CONFIG = setOf(
        "name", "uid", "setUidHash", "setParallelism", "setMaxParallelism", "forceNonParallel",
        "slotSharingGroup", "disableChaining", "startNewChain", "setDescription", "returns",
        "setBufferTimeout", "setResources", "setChainingStrategy",
        // DataStream V2
        "withName", "withUid", "withParallelism", "withMaxParallelism", "withSlotSharingGroup",
        "withDescription", "enableAsyncState",
    )

    /** Calls that only change how records are distributed to the next operator. */
    val PARTITION = setOf(
        "keyBy", "rebalance", "rescale", "shuffle", "forward", "global", "broadcast", "partitionCustom",
    )

    /** Intermediate steps folded into the next operator and shown as a tag on it. */
    val TAGGING = setOf(
        "window", "windowAll", "countWindow", "countWindowAll", "timeWindow", "timeWindowAll",
        "connect", "join", "coGroup", "intervalJoin",
    )

    /** Intermediate steps folded into the next operator without a tag. */
    val SILENT = setOf(
        "where", "equalTo", "between", "lowerBoundExclusive", "upperBoundExclusive", "trigger", "evictor",
        "allowedLateness", "sideOutputLateData", "inEventTime", "inProcessingTime",
    )

    val SINKS = setOf(
        "sinkTo", "addSink", "print", "printToErr", "writeAsText", "writeAsCsv", "writeToSocket",
        "writeUsingOutputFormat", "executeAndCollect", "collectAsync", "asQueryableState",
        // DataStream V2
        "toSink",
    )

    /** Source methods whose last String argument is the operator name. */
    val NAMED_SOURCES = setOf("fromSource", "addSource")

    fun isStreamType(type: PsiType?): Boolean {
        val cls = PsiUtil.resolveClassInClassTypeOnly(type) ?: return false
        return isStreamClass(cls)
    }

    fun isStreamClass(cls: PsiClass): Boolean {
        val qName = cls.qualifiedName ?: return false
        if (NOT_STREAMS.any { qName == it || qName.startsWith("$it.") }) return false
        return STREAM_PACKAGES.any { qName.startsWith(it) }
    }

    fun isSinkHandle(type: PsiType?): Boolean =
        PsiUtil.resolveClassInClassTypeOnly(type)?.qualifiedName == DATA_STREAM_SINK

    fun isApiClass(cls: PsiClass?): Boolean {
        val qName = cls?.qualifiedName ?: return false
        return API_PACKAGES.any { qName.startsWith(it) }
    }
}
