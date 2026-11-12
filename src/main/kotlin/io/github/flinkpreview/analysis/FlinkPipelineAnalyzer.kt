package io.github.flinkpreview.analysis

import com.intellij.openapi.util.TextRange
import com.intellij.psi.*
import com.intellij.psi.util.PsiTreeUtil
import com.intellij.psi.util.PsiUtil
import io.github.flinkpreview.model.*

class AnalysisResult(val graph: PipelineGraph?, val scope: String?, val message: String?)

/**
 * Builds a [PipelineGraph] from Java code using the DataStream API.
 *
 * Every stream-typed expression is evaluated to a [StreamRef]: the operators it reads from, plus
 * pending information (partitioning, window/join tags) that is folded into the next operator.
 * Results are memoised per (expression, context), so each call creates at most one node per context.
 * A context is created each time a helper method is inlined, binding its stream parameters to the
 * caller's arguments.
 *
 * Must be called inside a read action.
 */
class FlinkPipelineAnalyzer private constructor(private val project: com.intellij.openapi.project.Project) {

    companion object {
        private const val MAX_INLINE_DEPTH = 4
        private val CONSUMER_PACKAGES_TO_IGNORE = listOf("java.", "javax.", "kotlin.", "org.slf4j.", "org.apache.logging.")

        fun analyze(file: PsiJavaFile, caretOffset: Int? = null): AnalysisResult {
            val roots = PipelineScope.roots(file, caretOffset)
            if (roots.isEmpty()) {
                return AnalysisResult(null, null, "No Flink DataStream pipeline in ${file.name}.")
            }
            val analyzer = FlinkPipelineAnalyzer(file.project)
            roots.forEach(analyzer::analyzeRoot)
            val scope = roots.joinToString(", ") { "${it.containingClass?.name ?: file.name}.${it.name}()" }
            val graph = analyzer.toGraph()
            if (graph.nodes.isEmpty()) {
                return AnalysisResult(null, scope, "$scope uses Flink streams but builds no operators.")
            }
            return AnalysisResult(graph, scope, null)
        }
    }

    private data class Input(val nodeId: Int, val kind: EdgeKind = EdgeKind.FORWARD, val label: String? = null)

    private data class StreamRef(
        val inputs: List<Input>,
        val tags: List<String> = emptyList(),
        val pendingAnchors: List<PsiElement> = emptyList(),
    ) {
        /** The node this ref points to, if it is a plain operator output with nothing pending. */
        val directNode: Int?
            get() = inputs.singleOrNull()
                ?.takeIf { tags.isEmpty() && it.kind == EdgeKind.FORWARD && it.label == null }
                ?.nodeId

        companion object {
            fun of(nodeId: Int) = StreamRef(listOf(Input(nodeId)))
            fun merge(refs: List<StreamRef>): StreamRef? = if (refs.isEmpty()) null else StreamRef(
                refs.flatMap { it.inputs }.distinct(),
                refs.flatMap { it.tags }.distinct(),
                refs.flatMap { it.pendingAnchors }.distinct(),
            )
        }
    }

    private class Ctx(val id: Int, val bindings: Map<PsiVariable, StreamRef?>, val depth: Int, val stack: Set<PsiMethod>)

    /** Memo entry; a box so that "evaluated to null" is distinguishable from "not evaluated yet". */
    private class Box(val ref: StreamRef?)

    private val nodes = ArrayList<PipelineNode>()
    private val edges = LinkedHashSet<PipelineEdge>()
    private val memo = HashMap<Any, Box>()
    private val inProgress = HashSet<Any>()
    private var contextCount = 0
    private val fieldCtx = Ctx(0, emptyMap(), 0, emptySet())
    private val pointers = SmartPointerManager.getInstance(project)
    private val documents = PsiDocumentManager.getInstance(project)

    private fun toGraph() = PipelineGraph(nodes, edges.toList())

    private fun newCtx(bindings: Map<PsiVariable, StreamRef?>, depth: Int, stack: Set<PsiMethod>) =
        Ctx(++contextCount, bindings, depth, stack)

    private fun analyzeRoot(method: PsiMethod) = analyzeBody(method, newCtx(emptyMap(), 0, setOf(method)))

    /** Evaluates every stream-related call in the method, so that statements like `s.sinkTo(...)` are found. */
    private fun analyzeBody(method: PsiMethod, ctx: Ctx) {
        val body = method.body ?: return
        for (call in PsiTreeUtil.collectElementsOfType(body, PsiMethodCallExpression::class.java)) {
            val enclosingClass = PsiTreeUtil.getParentOfType(call, PsiClass::class.java)
            if (enclosingClass != null && PsiTreeUtil.isAncestor(body, enclosingClass, true)) continue // anonymous/local class
            if (isRelevant(call)) evalCall(call, ctx)
        }
    }

    private fun isRelevant(call: PsiMethodCallExpression): Boolean {
        if (FlinkApi.isStreamType(call.methodExpression.qualifierExpression?.type)) return true
        if (FlinkApi.isStreamType(call.type)) return true
        return call.argumentList.expressions.any { FlinkApi.isStreamType(it.type) }
    }

    // ---------------------------------------------------------------- expressions

    /** Like [evalExpr], but stream-typed expressions it can't follow become "external input" nodes. */
    private fun evalStream(expr: PsiExpression, ctx: Ctx): StreamRef? {
        evalExpr(expr, ctx)?.let { return it }
        if (!FlinkApi.isStreamType(expr.type)) return null
        return external(expr, ExpressionSummary.of(expr), ctx)
    }

    private fun evalExpr(expr: PsiExpression?, ctx: Ctx): StreamRef? {
        return when (val e = PsiUtil.skipParenthesizedExprDown(expr)) {
            null -> null
            is PsiMethodCallExpression -> evalCall(e, ctx)
            is PsiTypeCastExpression -> evalExpr(e.operand, ctx)
            is PsiConditionalExpression ->
                StreamRef.merge(listOfNotNull(evalExpr(e.thenExpression, ctx), evalExpr(e.elseExpression, ctx)))
            is PsiAssignmentExpression -> evalExpr(e.rExpression, ctx)
            is PsiMethodReferenceExpression -> null
            is PsiReferenceExpression -> evalReference(e, ctx)
            else -> null
        }
    }

    private fun evalReference(ref: PsiReferenceExpression, ctx: Ctx): StreamRef? {
        val target = ref.resolve() as? PsiVariable ?: return null
        if (!FlinkApi.isStreamType(target.type)) return null
        return when (target) {
            is PsiParameter ->
                if (ctx.bindings.containsKey(target)) ctx.bindings[target] else external(target, target.name, ctx)
            is PsiLocalVariable ->
                reachingDefinition(target, ref)?.let { evalStream(it, ctx) } ?: external(target, target.name, ctx)
            is PsiField ->
                fieldDefinition(target)?.let { evalStream(it, fieldCtx) } ?: external(target, target.name, fieldCtx)
            else -> null
        }
    }

    /** The expression last assigned to [variable] before [usage]: the initializer or a completed assignment. */
    private fun reachingDefinition(variable: PsiLocalVariable, usage: PsiElement): PsiExpression? {
        val scope = PsiUtil.getVariableCodeBlock(variable, null) ?: variable.parent ?: return variable.initializer
        val usageOffset = usage.textRange.startOffset
        var best = variable.initializer
        var bestEnd = -1
        for (assignment in PsiTreeUtil.findChildrenOfType(scope, PsiAssignmentExpression::class.java)) {
            if (assignment.operationTokenType != JavaTokenType.EQ) continue
            val lhs = PsiUtil.skipParenthesizedExprDown(assignment.lExpression) as? PsiReferenceExpression ?: continue
            val end = assignment.textRange.endOffset
            if (end <= usageOffset && end > bestEnd && lhs.isReferenceTo(variable)) {
                best = assignment.rExpression
                bestEnd = end
            }
        }
        return best
    }

    private fun fieldDefinition(field: PsiField): PsiExpression? {
        field.initializer?.let { return it }
        val cls = field.containingClass ?: return null
        return PsiTreeUtil.findChildrenOfType(cls, PsiAssignmentExpression::class.java)
            .lastOrNull { a ->
                a.operationTokenType == JavaTokenType.EQ &&
                    (PsiUtil.skipParenthesizedExprDown(a.lExpression) as? PsiReferenceExpression)?.isReferenceTo(field) == true
            }?.rExpression
    }

    private fun external(key: PsiElement, label: String, ctx: Ctx): StreamRef =
        memoized(Triple(key, "external", ctx.id)) {
            val node = newNode(NodeKind.EXTERNAL, label)
            addAnchor(node, key)
            StreamRef.of(node.id)
        }!!

    // ---------------------------------------------------------------- calls

    private fun evalCall(call: PsiMethodCallExpression, ctx: Ctx): StreamRef? =
        memoized(Pair(call, ctx.id)) { computeCall(call, ctx) }

    private inline fun memoized(key: Any, compute: () -> StreamRef?): StreamRef? {
        memo[key]?.let { return it.ref }
        if (!inProgress.add(key)) return null // recursion through a cycle
        try {
            val result = compute()
            memo[key] = Box(result)
            return result
        } finally {
            inProgress.remove(key)
        }
    }

    private fun computeCall(call: PsiMethodCallExpression, ctx: Ctx): StreamRef? {
        val method = resolve(call) ?: return null
        val qualifier = call.methodExpression.qualifierExpression
        if (qualifier != null && FlinkApi.isStreamType(qualifier.type)) {
            val upstream = evalStream(qualifier, ctx) ?: return null
            return applyStreamMethod(call, method, upstream, ctx)
        }

        val args = call.argumentList.expressions
        val returnsStream = returnsStream(call, method)
        val hasStreamArgs = args.any { FlinkApi.isStreamType(it.type) }
        if (!returnsStream && !hasStreamArgs) return null

        if (canInline(method, ctx)) return inline(call, method, ctx)

        val streamInputs = streamArgRefs(args, ctx)
        return when {
            returnsStream && streamInputs.isEmpty() -> {
                // env.fromSource(...), env.fromElements(...), tableEnv.toDataStream(...), library factories
                val node = createNode(NodeKind.SOURCE, label(method.name, args), call, StreamRef(emptyList()))
                if (method.name in FlinkApi.NAMED_SOURCES && args.size >= 2) {
                    ExpressionSummary.stringValue(args.last())?.let { node.name = it }
                }
                StreamRef.of(node.id)
            }
            returnsStream -> {
                // Static helpers such as AsyncDataStream.unorderedWait(stream, ...)
                val kind = if (FlinkApi.isSinkHandle(call.type ?: method.returnType)) NodeKind.SINK else NodeKind.OPERATOR
                StreamRef.of(createNode(kind, label(method.name, args), call, StreamRef.merge(streamInputs)!!).id)
            }
            isIgnoredConsumer(method) -> null
            else -> {
                // A library method that consumes a stream, e.g. a sink utility.
                createNode(NodeKind.SINK, label(method.name, args), call, StreamRef.merge(streamInputs)!!)
                null
            }
        }
    }

    /**
     * Resolves the called method, falling back to the best candidate when the call doesn't fully
     * type-check, which is common while code is being edited.
     */
    private fun resolve(call: PsiMethodCallExpression): PsiMethod? =
        call.resolveMethod()
            ?: call.methodExpression.multiResolve(false).firstNotNullOfOrNull { it.element as? PsiMethod }

    /** Uses the declared return type when the call's type can't be inferred (e.g. ambiguous overloads). */
    private fun returnsStream(call: PsiMethodCallExpression, method: PsiMethod): Boolean =
        FlinkApi.isStreamType(call.type) || FlinkApi.isStreamType(method.returnType)

    private fun applyStreamMethod(call: PsiMethodCallExpression, method: PsiMethod, upstream: StreamRef, ctx: Ctx): StreamRef? {
        val name = method.name
        val args = call.argumentList.expressions
        val anchor = anchorOf(call)
        return when {
            name in FlinkApi.CONFIG -> {
                applyConfig(name, args, upstream, anchor)
                upstream
            }
            name in FlinkApi.PARTITION -> {
                val label = when (name) {
                    "keyBy" -> label(name, args)
                    "partitionCustom" -> "partitionCustom(${ExpressionSummary.of(args.firstOrNull(), 20)})"
                    else -> name
                }
                val kind = when (name) {
                    "broadcast" -> EdgeKind.BROADCAST
                    "forward" -> EdgeKind.FORWARD
                    else -> EdgeKind.PARTITION
                }
                upstream.copy(
                    inputs = upstream.inputs.map {
                        // A repartitioned side output stays a side output, with both facts in the label.
                        if (it.kind == EdgeKind.SIDE_OUTPUT) it.copy(label = listOfNotNull(it.label, label).joinToString(", "))
                        else it.copy(kind = kind, label = label)
                    },
                    pendingAnchors = upstream.pendingAnchors + anchor,
                )
            }
            name == "getSideOutput" -> upstream.copy(
                inputs = upstream.inputs.map {
                    it.copy(kind = EdgeKind.SIDE_OUTPUT, label = "side output ${ExpressionSummary.of(args.firstOrNull(), 24)}")
                },
                tags = emptyList(),
                pendingAnchors = upstream.pendingAnchors + anchor,
            )
            name == "getFirst" || name == "getSecond" -> upstream.copy( // DataStream V2 two-output streams
                inputs = upstream.inputs.map { it.copy(label = if (name == "getFirst") "output 1" else "output 2") },
                pendingAnchors = upstream.pendingAnchors + anchor,
            )
            name in FlinkApi.TAGGING -> {
                val other = streamArgRefs(args, ctx)
                upstream.copy(
                    inputs = (upstream.inputs + other.flatMap { it.inputs }).distinct(),
                    tags = upstream.tags + tag(name, args),
                    pendingAnchors = upstream.pendingAnchors + anchor,
                )
            }
            name in FlinkApi.SILENT -> upstream.copy(pendingAnchors = upstream.pendingAnchors + anchor)
            name in FlinkApi.SINKS || FlinkApi.isSinkHandle(call.type ?: method.returnType) -> {
                val inputs = StreamRef.merge(listOf(upstream) + streamArgRefs(args, ctx))!!
                StreamRef.of(createNode(NodeKind.SINK, label(name, args), call, inputs).id)
            }
            returnsStream(call, method) -> {
                val inputs = StreamRef.merge(listOf(upstream) + streamArgRefs(args, ctx))!!
                val kind = if (name == "union") NodeKind.UNION else NodeKind.OPERATOR
                StreamRef.of(createNode(kind, label(name, args), call, inputs).id)
            }
            else -> null // getType(), getId(), getExecutionEnvironment(), ...
        }
    }

    private fun applyConfig(name: String, args: Array<PsiExpression>, upstream: StreamRef, anchor: PsiElement) {
        val node = upstream.directNode?.let { nodes[it] } ?: return
        val arg = args.firstOrNull()
        when (name) {
            "name", "withName" -> node.name = ExpressionSummary.stringValue(arg) ?: ExpressionSummary.of(arg)
            "uid", "withUid", "setUidHash" -> node.uid = ExpressionSummary.stringValue(arg) ?: ExpressionSummary.of(arg)
            "setParallelism", "withParallelism" -> node.parallelism = ExpressionSummary.valueText(arg)
            "forceNonParallel" -> node.parallelism = "1"
            "setMaxParallelism", "withMaxParallelism" -> node.maxParallelism = ExpressionSummary.valueText(arg)
            "slotSharingGroup", "withSlotSharingGroup" -> node.slotSharingGroup = ExpressionSummary.valueText(arg)
            "disableChaining" -> node.chaining = "no chaining"
            "startNewChain" -> node.chaining = "new chain"
            "setDescription", "withDescription" -> node.description = ExpressionSummary.stringValue(arg)
        }
        addAnchor(node, anchor)
    }

    // ---------------------------------------------------------------- helper methods

    private fun canInline(method: PsiMethod, ctx: Ctx): Boolean =
        method !is PsiCompiledElement &&
            method.body != null &&
            !FlinkApi.isApiClass(method.containingClass) &&
            ctx.depth < MAX_INLINE_DEPTH &&
            method !in ctx.stack

    /** Analyses a helper method with its stream parameters bound to the caller's arguments. */
    private fun inline(call: PsiMethodCallExpression, method: PsiMethod, ctx: Ctx): StreamRef? {
        val args = call.argumentList.expressions
        val bindings = HashMap<PsiVariable, StreamRef?>()
        method.parameterList.parameters.forEachIndexed { i, parameter ->
            if (FlinkApi.isStreamType(parameter.type)) {
                bindings[parameter] = args.getOrNull(i)?.let { evalStream(it, ctx) }
            }
        }
        val inner = newCtx(bindings, ctx.depth + 1, ctx.stack + method)
        analyzeBody(method, inner)

        if (!FlinkApi.isStreamType(method.returnType)) return null
        val returns = PsiTreeUtil.findChildrenOfType(method.body, PsiReturnStatement::class.java).filter {
            PsiTreeUtil.getParentOfType(it, PsiMethod::class.java, true, PsiLambdaExpression::class.java, PsiClass::class.java) == method
        }
        return StreamRef.merge(returns.mapNotNull { r -> r.returnValue?.let { evalStream(it, inner) } })
    }

    // ---------------------------------------------------------------- graph building

    private fun streamArgRefs(args: Array<PsiExpression>, ctx: Ctx): List<StreamRef> =
        args.filter { FlinkApi.isStreamType(it.type) }.mapNotNull { evalStream(it, ctx) }

    private fun isIgnoredConsumer(method: PsiMethod): Boolean {
        val qName = method.containingClass?.qualifiedName ?: return false
        return CONSUMER_PACKAGES_TO_IGNORE.any { qName.startsWith(it) }
    }

    private fun newNode(kind: NodeKind, label: String): PipelineNode =
        PipelineNode(nodes.size, kind, label).also { nodes += it }

    private fun createNode(kind: NodeKind, label: String, call: PsiMethodCallExpression, inputs: StreamRef): PipelineNode {
        val node = newNode(kind, label)
        node.tags += inputs.tags
        addAnchor(node, anchorOf(call))
        inputs.pendingAnchors.forEach { addAnchor(node, it) }
        for (input in inputs.inputs) edges += PipelineEdge(input.nodeId, node.id, input.kind, input.label)
        return node
    }

    /** `name(arg, arg)` with stream arguments left out and long argument lists shortened. */
    private fun label(name: String, args: Array<PsiExpression>, argMax: Int = 26, max: Int = 48): String {
        val shown = args.filterNot { FlinkApi.isStreamType(it.type) }
        val parts = shown.take(2).map { ExpressionSummary.of(it, argMax) } + if (shown.size > 2) listOf("…") else emptyList()
        return ExpressionSummary.truncate("$name(${parts.joinToString(", ")})", max)
    }

    private fun tag(name: String, args: Array<PsiExpression>): String = when (name) {
        "connect", "join", "coGroup", "intervalJoin" -> name
        else -> label(name, args, argMax = 40, max = 56)
    }

    private fun anchorOf(call: PsiMethodCallExpression): PsiElement = call.methodExpression.referenceNameElement ?: call

    private fun addAnchor(node: PipelineNode, element: PsiElement) {
        val file = element.containingFile ?: return
        val call = element.parent?.parent as? PsiMethodCallExpression
        val start = element.textRange.startOffset
        val end = call?.argumentList?.textRange?.endOffset ?: element.textRange.endOffset
        if (node.anchors.any { it.range.startOffset == start && it.fileUrl == (file.virtualFile?.url ?: "") }) return
        val line = documents.getDocument(file)?.let { it.getLineNumber(start) + 1 } ?: 0
        node.anchors += SourceAnchor(
            pointer = pointers.createSmartPsiElementPointer(element),
            fileUrl = file.virtualFile?.url ?: "",
            fileName = file.name,
            range = TextRange(start, maxOf(start, end)),
            line = line,
        )
    }
}
