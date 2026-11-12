package io.github.flinkpreview.model

import com.intellij.openapi.util.TextRange
import com.intellij.psi.PsiElement
import com.intellij.psi.SmartPsiElementPointer

enum class NodeKind { SOURCE, OPERATOR, SINK, UNION, EXTERNAL }

/** How records travel along an edge. */
enum class EdgeKind { FORWARD, PARTITION, BROADCAST, SIDE_OUTPUT }

/** A piece of code that contributed to a node (the operator call itself, or a .name()/.uid()/.window() call). */
class SourceAnchor(
    val pointer: SmartPsiElementPointer<PsiElement>,
    val fileUrl: String,
    val fileName: String,
    val range: TextRange,
    val line: Int,
)

class PipelineNode(val id: Int, val kind: NodeKind, val label: String) {
    val tags = mutableListOf<String>()
    var name: String? = null
    var uid: String? = null
    var parallelism: String? = null
    var maxParallelism: String? = null
    var slotSharingGroup: String? = null
    var chaining: String? = null
    var description: String? = null
    val anchors = mutableListOf<SourceAnchor>()

    val title: String get() = name ?: label
}

data class PipelineEdge(val from: Int, val to: Int, val kind: EdgeKind, val label: String?)

class PipelineGraph(val nodes: List<PipelineNode>, val edges: List<PipelineEdge>) {

    /** Stable, human-readable text form, used by tests and handy for debugging. */
    fun dump(): String = buildString {
        for (n in nodes) {
            append(n.kind).append(' ').append(n.label)
            n.name?.let { append(" name=").append(it) }
            if (n.tags.isNotEmpty()) append(" tags=").append(n.tags)
            n.uid?.let { append(" uid=").append(it) }
            n.parallelism?.let { append(" p=").append(it) }
            n.chaining?.let { append(" chaining=").append(it) }
            append('\n')
        }
        for (e in edges) {
            append(nodes[e.from].title).append(" -> ").append(nodes[e.to].title)
            if (e.kind != EdgeKind.FORWARD) append(' ').append(e.kind)
            e.label?.let { append(" [").append(it).append(']') }
            append('\n')
        }
    }
}
