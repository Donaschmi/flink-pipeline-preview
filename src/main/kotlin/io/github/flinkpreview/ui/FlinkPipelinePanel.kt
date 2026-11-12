package io.github.flinkpreview.ui

import com.intellij.ide.ui.LafManagerListener
import com.intellij.openapi.Disposable
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.application.ModalityState
import com.intellij.openapi.application.ReadAction
import com.intellij.openapi.application.runReadAction
import com.intellij.openapi.editor.Editor
import com.intellij.openapi.editor.EditorFactory
import com.intellij.openapi.editor.event.CaretEvent
import com.intellij.openapi.editor.event.CaretListener
import com.intellij.openapi.editor.event.DocumentEvent
import com.intellij.openapi.editor.event.DocumentListener
import com.intellij.openapi.fileEditor.FileDocumentManager
import com.intellij.openapi.fileEditor.FileEditorManager
import com.intellij.openapi.fileEditor.FileEditorManagerEvent
import com.intellij.openapi.fileEditor.FileEditorManagerListener
import com.intellij.openapi.fileEditor.OpenFileDescriptor
import com.intellij.openapi.project.DumbService
import com.intellij.openapi.project.Project
import com.intellij.openapi.util.Disposer
import com.intellij.psi.PsiDocumentManager
import com.intellij.psi.PsiJavaFile
import com.intellij.psi.PsiMethod
import com.intellij.psi.util.PsiTreeUtil
import com.intellij.ui.ColorUtil
import com.intellij.ui.JBColor
import com.intellij.ui.components.JBLabel
import com.intellij.ui.jcef.JBCefApp
import com.intellij.ui.jcef.JBCefBrowser
import com.intellij.ui.jcef.JBCefBrowserBase
import com.intellij.ui.jcef.JBCefJSQuery
import com.intellij.util.Alarm
import com.intellij.util.concurrency.AppExecutorUtil
import com.intellij.util.ui.JBUI
import com.intellij.util.ui.UIUtil
import io.github.flinkpreview.analysis.FlinkPipelineAnalyzer
import io.github.flinkpreview.model.SourceAnchor
import org.cef.browser.CefBrowser
import org.cef.browser.CefFrame
import org.cef.handler.CefLoadHandlerAdapter
import java.awt.BorderLayout
import javax.swing.JPanel
import javax.swing.SwingConstants

/**
 * Renders the pipeline of the selected Java editor in an embedded browser and keeps it in sync:
 * re-analyses (debounced, off the EDT) when the file, the document or the caret's method changes,
 * highlights the node under the caret, and navigates to the code when a node is clicked.
 */
class FlinkPipelinePanel(private val project: Project) : JPanel(BorderLayout()), Disposable {

    private class Rendered(val json: String, val anchors: Map<Int, List<SourceAnchor>>)

    private val browser: JBCefBrowser? = if (JBCefApp.isSupported()) JBCefBrowser.createBuilder().build() else null
    private val bridge: JBCefJSQuery? = browser?.let { JBCefJSQuery.create(it as JBCefBrowserBase) }
    private val refreshAlarm = Alarm(Alarm.ThreadToUse.SWING_THREAD, this)

    private var pageReady = false
    private var current = Rendered(GraphJson.message("Open a Java file that builds a Flink pipeline."), emptyMap())
    private var highlighted: Int? = null
    private var caretMethod: PsiMethod? = null

    init {
        if (browser != null && bridge != null) {
            Disposer.register(this, browser)
            Disposer.register(this, bridge)
            bridge.addHandler { message ->
                ApplicationManager.getApplication().invokeLater({ onMessage(message) }, ModalityState.any())
                null
            }
            browser.jbCefClient.addLoadHandler(object : CefLoadHandlerAdapter() {
                override fun onLoadEnd(cefBrowser: CefBrowser, frame: CefFrame, httpStatusCode: Int) {
                    if (!frame.isMain) return
                    cefBrowser.executeJavaScript(
                        "window.flinkBridge = function(m) { ${bridge.inject("m")} };",
                        cefBrowser.url, 0,
                    )
                    ApplicationManager.getApplication().invokeLater({
                        pageReady = true
                        push()
                    }, ModalityState.any())
                }
            }, browser.cefBrowser)
            browser.loadHTML(loadPage())
            add(browser.component, BorderLayout.CENTER)
        } else {
            add(JBLabel("The Flink pipeline view needs JCEF, which this IDE runtime doesn't provide.", SwingConstants.CENTER))
        }
        installListeners()
        scheduleRefresh(0)
    }

    override fun dispose() = Unit

    // ---------------------------------------------------------------- events

    private fun installListeners() {
        val connection = project.messageBus.connect(this)
        connection.subscribe(FileEditorManagerListener.FILE_EDITOR_MANAGER, object : FileEditorManagerListener {
            override fun selectionChanged(event: FileEditorManagerEvent) = scheduleRefresh(0)
        })
        connection.subscribe(DumbService.DUMB_MODE, object : DumbService.DumbModeListener {
            override fun exitDumbMode() = scheduleRefresh(0)
        })
        ApplicationManager.getApplication().messageBus.connect(this)
            .subscribe(LafManagerListener.TOPIC, LafManagerListener { push() })

        val multicaster = EditorFactory.getInstance().eventMulticaster
        multicaster.addDocumentListener(object : DocumentListener {
            override fun documentChanged(event: DocumentEvent) {
                if (selectedEditor()?.document == event.document) scheduleRefresh()
            }
        }, this)
        multicaster.addCaretListener(object : CaretListener {
            override fun caretPositionChanged(event: CaretEvent) {
                if (event.editor != selectedEditor()) return
                highlightCaret(event.editor)
                val method = methodAtCaret(event.editor)
                if (method != caretMethod) {
                    caretMethod = method
                    scheduleRefresh()
                }
            }
        }, this)
    }

    private fun onMessage(message: String) {
        if (message.startsWith("nav:")) message.removePrefix("nav:").toIntOrNull()?.let(::navigateTo)
    }

    // ---------------------------------------------------------------- analysis

    private fun scheduleRefresh(delayMs: Int = 350) {
        refreshAlarm.cancelAllRequests()
        refreshAlarm.addRequest(::refresh, delayMs)
    }

    private fun refresh() {
        val editor = selectedEditor()
        if (editor == null) {
            show(Rendered(GraphJson.message("Open a Java file that builds a Flink pipeline."), emptyMap()))
            return
        }
        val document = editor.document
        val caret = editor.caretModel.offset
        ReadAction.nonBlocking<Rendered> {
            val file = PsiDocumentManager.getInstance(project).getPsiFile(document) as? PsiJavaFile
                ?: return@nonBlocking Rendered(GraphJson.message("The pipeline view works on Java files."), emptyMap())
            val result = FlinkPipelineAnalyzer.analyze(file, caret)
            val anchors = result.graph?.nodes?.associate { it.id to it.anchors.toList() } ?: emptyMap()
            Rendered(GraphJson.of(result), anchors)
        }
            .inSmartMode(project)
            .withDocumentsCommitted(project)
            .expireWith(this)
            .coalesceBy(this)
            .finishOnUiThread(ModalityState.defaultModalityState()) { rendered ->
                show(rendered)
                selectedEditor()?.let(::highlightCaret)
            }
            .submit(AppExecutorUtil.getAppExecutorService())
    }

    private fun show(rendered: Rendered) {
        current = rendered
        highlighted = null
        push()
    }

    // ---------------------------------------------------------------- browser

    private fun push() {
        if (!pageReady) return
        execute("window.flinkPreview && window.flinkPreview.render(${current.json}, ${themeJson()});")
    }

    private fun execute(js: String) {
        val cef = browser?.cefBrowser ?: return
        cef.executeJavaScript(js, cef.url, 0)
    }

    private fun highlightCaret(editor: Editor) {
        val url = FileDocumentManager.getInstance().getFile(editor.document)?.url ?: return
        val offset = editor.caretModel.offset
        val match = current.anchors.entries
            .flatMap { (id, anchors) -> anchors.filter { it.fileUrl == url && it.range.containsOffset(offset) }.map { id to it } }
            .minByOrNull { it.second.range.length }
            ?.first
        if (match == highlighted) return
        highlighted = match
        if (pageReady) execute("window.flinkPreview && window.flinkPreview.highlight(${match ?: "null"});")
    }

    private fun navigateTo(nodeId: Int) {
        val anchor = current.anchors[nodeId]?.firstOrNull() ?: return
        val element = anchor.pointer.element ?: return
        val file = element.containingFile?.virtualFile ?: return
        OpenFileDescriptor(project, file, element.textOffset).navigate(true)
    }

    private fun themeJson(): String {
        val font = UIUtil.getLabelFont()
        fun hex(c: java.awt.Color) = GraphJson.str(ColorUtil.toHtmlColor(c))
        return "{\"dark\":${!JBColor.isBright()}" +
            ",\"background\":${hex(UIUtil.getPanelBackground())}" +
            ",\"foreground\":${hex(UIUtil.getLabelForeground())}" +
            ",\"muted\":${hex(UIUtil.getContextHelpForeground())}" +
            ",\"border\":${hex(JBColor.border())}" +
            ",\"accent\":${hex(JBUI.CurrentTheme.Link.Foreground.ENABLED)}" +
            ",\"fontFamily\":${GraphJson.str(font.family)}" +
            ",\"fontSize\":${font.size}}"
    }

    private fun loadPage(): String =
        javaClass.getResourceAsStream("/flinkpreview/view.html")!!.bufferedReader().use { it.readText() }

    // ---------------------------------------------------------------- helpers

    private fun selectedEditor(): Editor? = FileEditorManager.getInstance(project).selectedTextEditor

    private fun methodAtCaret(editor: Editor): PsiMethod? = runReadAction {
        val file = PsiDocumentManager.getInstance(project).getCachedPsiFile(editor.document) as? PsiJavaFile
        file?.findElementAt(editor.caretModel.offset)?.let { PsiTreeUtil.getParentOfType(it, PsiMethod::class.java) }
    }
}
