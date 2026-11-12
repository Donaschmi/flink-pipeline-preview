package io.github.flinkpreview.ui

import io.github.flinkpreview.analysis.AnalysisResult

/** Minimal JSON writer for the payload sent to the web view (avoids depending on a JSON library). */
object GraphJson {

    fun message(text: String): String = "{\"message\":${str(text)}}"

    fun of(result: AnalysisResult): String {
        val graph = result.graph ?: return message(result.message ?: "Nothing to show.")
        val sb = StringBuilder()
        sb.append("{\"scope\":").append(str(result.scope))
        sb.append(",\"nodes\":[")
        graph.nodes.forEachIndexed { i, n ->
            if (i > 0) sb.append(',')
            val anchor = n.anchors.firstOrNull()
            sb.append("{\"id\":").append(n.id)
                .append(",\"kind\":").append(str(n.kind.name))
                .append(",\"title\":").append(str(n.title))
                .append(",\"label\":").append(str(n.label))
                .append(",\"named\":").append(n.name != null)
                .append(",\"tags\":[").append(n.tags.joinToString(",") { str(it) }).append(']')
                .append(",\"uid\":").append(str(n.uid))
                .append(",\"parallelism\":").append(str(n.parallelism))
                .append(",\"maxParallelism\":").append(str(n.maxParallelism))
                .append(",\"slotSharingGroup\":").append(str(n.slotSharingGroup))
                .append(",\"chaining\":").append(str(n.chaining))
                .append(",\"description\":").append(str(n.description))
                .append(",\"location\":").append(str(anchor?.let { "${it.fileName}:${it.line}" }))
                .append('}')
        }
        sb.append("],\"edges\":[")
        graph.edges.forEachIndexed { i, e ->
            if (i > 0) sb.append(',')
            sb.append("{\"from\":").append(e.from)
                .append(",\"to\":").append(e.to)
                .append(",\"kind\":").append(str(e.kind.name))
                .append(",\"label\":").append(str(e.label))
                .append('}')
        }
        sb.append("]}")
        return sb.toString()
    }

    fun str(s: String?): String {
        if (s == null) return "null"
        val sb = StringBuilder(s.length + 2).append('"')
        for (c in s) {
            when (c) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                '<' -> sb.append("\\u003c")
                '\u2028', '\u2029' -> sb.append(String.format("\\u%04x", c.code))
                else -> if (c < ' ') sb.append(String.format("\\u%04x", c.code)) else sb.append(c)
            }
        }
        return sb.append('"').toString()
    }
}
