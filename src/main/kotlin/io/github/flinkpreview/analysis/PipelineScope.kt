package io.github.flinkpreview.analysis

import com.intellij.psi.*
import com.intellij.psi.util.PsiTreeUtil

/**
 * Decides which methods of a file to analyse.
 *
 * Methods that build a pipeline and are not called by another pipeline method of the same file are
 * "roots" (usually `main`); helper methods are reached by inlining from their callers. When the caret
 * is in a root, or in a helper called by some roots, only those roots are shown, so the view stays on
 * the job you are working on.
 */
object PipelineScope {

    fun roots(file: PsiJavaFile, caretOffset: Int?): List<PsiMethod> {
        val pipelineMethods = PsiTreeUtil.findChildrenOfType(file, PsiMethod::class.java)
            .filter { it.body != null && containsPipeline(it) }
        if (pipelineMethods.isEmpty()) return emptyList()

        val callees = pipelineMethods.associateWith { m -> calledMethods(m).filter { it in pipelineMethods }.toSet() }
        val called = callees.values.flatten().toSet()
        val roots = pipelineMethods.filter { it !in called }.ifEmpty { pipelineMethods.take(1) }

        val caretMethod = caretOffset?.let { methodAt(file, it, pipelineMethods) } ?: return roots
        if (caretMethod in roots) return listOf(caretMethod)
        val reaching = roots.filter { root -> reaches(root, caretMethod, callees) }
        return reaching.ifEmpty { listOf(caretMethod) }
    }

    /** True if the method body creates, transforms or consumes a Flink stream. */
    fun containsPipeline(method: PsiMethod): Boolean {
        val body = method.body ?: return false
        return PsiTreeUtil.findChildrenOfType(body, PsiMethodCallExpression::class.java).any { call ->
            FlinkApi.isStreamType(call.type) ||
                FlinkApi.isStreamType(call.methodExpression.qualifierExpression?.type)
        }
    }

    private fun methodAt(file: PsiJavaFile, offset: Int, candidates: List<PsiMethod>): PsiMethod? {
        var method = PsiTreeUtil.getParentOfType(file.findElementAt(offset), PsiMethod::class.java, false)
        while (method != null) {
            if (method in candidates) return method
            method = PsiTreeUtil.getParentOfType(method, PsiMethod::class.java, true)
        }
        return null
    }

    private fun calledMethods(method: PsiMethod): List<PsiMethod> =
        PsiTreeUtil.findChildrenOfType(method.body, PsiMethodCallExpression::class.java).mapNotNull { it.resolveMethod() }

    private fun reaches(from: PsiMethod, target: PsiMethod, callees: Map<PsiMethod, Set<PsiMethod>>): Boolean {
        val seen = HashSet<PsiMethod>()
        val queue = ArrayDeque(listOf(from))
        while (queue.isNotEmpty()) {
            val m = queue.removeFirst()
            if (m == target) return true
            if (seen.add(m)) queue.addAll(callees[m].orEmpty())
        }
        return false
    }
}
