package io.github.flinkpreview.analysis

import com.intellij.psi.*
import com.intellij.psi.util.PsiUtil

/**
 * Turns argument expressions into short labels such as `Tokenizer`, `λ`, `KafkaSource`
 * or `TumblingEventTimeWindows(10s)`.
 */
object ExpressionSummary {

    private val DURATION_UNITS = mapOf(
        "ofMillis" to "ms", "milliseconds" to "ms",
        "ofSeconds" to "s", "seconds" to "s",
        "ofMinutes" to "min", "minutes" to "min",
        "ofHours" to "h", "hours" to "h",
        "ofDays" to "d", "days" to "d",
    )

    private val WHITESPACE = Regex("\\s+")

    fun of(expr: PsiExpression?, max: Int = 32): String {
        val e = PsiUtil.skipParenthesizedExprDown(expr) ?: return ""
        val text = when (e) {
            is PsiLambdaExpression -> "λ"
            is PsiMethodReferenceExpression -> e.text
            is PsiLiteralExpression -> e.text
            is PsiNewExpression -> newExpression(e)
            is PsiMethodCallExpression -> call(e)
            is PsiReferenceExpression -> if (e.qualifierExpression == null) e.referenceName ?: e.text else compact(e.text)
            is PsiTypeCastExpression -> return of(e.operand, max)
            else -> compact(e.text)
        }
        return truncate(text, max)
    }

    /** The value of a string literal or compile-time string constant, e.g. `"Tokenizer"` or `Names.TOKENIZER`. */
    fun stringValue(expr: PsiExpression?): String? {
        val e = PsiUtil.skipParenthesizedExprDown(expr) ?: return null
        return JavaPsiFacade.getInstance(e.project).constantEvaluationHelper.computeConstantExpression(e) as? String
    }

    /** A literal or constant rendered as plain text (used for parallelism, uid, ...). */
    fun valueText(expr: PsiExpression?): String {
        val e = PsiUtil.skipParenthesizedExprDown(expr) ?: return ""
        val constant = JavaPsiFacade.getInstance(e.project).constantEvaluationHelper.computeConstantExpression(e)
        return constant?.toString() ?: of(e)
    }

    fun truncate(text: String, max: Int): String = if (text.length <= max) text else text.take(max - 1) + "…"

    private fun compact(text: String) = text.replace(WHITESPACE, " ")

    private fun newExpression(e: PsiNewExpression): String {
        e.anonymousClass?.let { return (it.baseClassReference.referenceName ?: "anonymous") + " {…}" }
        if (e.arrayInitializer != null || e.arrayDimensions.isNotEmpty()) return "[…]"
        return e.classReference?.referenceName ?: compact(e.text)
    }

    private fun call(e: PsiMethodCallExpression): String {
        duration(e)?.let { return it }
        var current: PsiExpression = e
        while (current is PsiMethodCallExpression) {
            val qualifier = PsiUtil.skipParenthesizedExprDown(current.methodExpression.qualifierExpression)
            if (qualifier == null) {
                val suffix = if (current === e) "" else "…"
                return "${current.methodExpression.referenceName}()$suffix"
            }
            current = qualifier
        }
        return when (current) {
            is PsiNewExpression -> newExpression(current)
            is PsiReferenceExpression -> {
                val cls = current.resolve() as? PsiClass ?: return compact(e.text)
                val base = cls.name ?: current.text
                // Builders and factories: `KafkaSource.builder()...build()` -> KafkaSource,
                // `TumblingEventTimeWindows.of(Duration.ofSeconds(10))` -> TumblingEventTimeWindows(10s)
                val args = e.argumentList.expressions
                val durations = args.map { duration(it) }
                if (args.isNotEmpty() && durations.all { it != null }) "$base(${durations.joinToString(", ")})" else base
            }
            else -> compact(e.text)
        }
    }

    private fun duration(expr: PsiExpression?): String? {
        val call = PsiUtil.skipParenthesizedExprDown(expr) as? PsiMethodCallExpression ?: return null
        val qualifier = call.methodExpression.qualifierExpression?.text ?: return null
        val isDurationClass = qualifier == "Duration" || qualifier == "Time" ||
            qualifier.endsWith(".Duration") || qualifier.endsWith(".Time")
        if (!isDurationClass) return null
        val unit = DURATION_UNITS[call.methodExpression.referenceName] ?: return null
        val arg = call.argumentList.expressions.singleOrNull() ?: return null
        return valueText(arg) + unit
    }
}
