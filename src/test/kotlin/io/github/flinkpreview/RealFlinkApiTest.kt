package io.github.flinkpreview

import com.intellij.openapi.module.Module
import com.intellij.openapi.projectRoots.JavaSdk
import com.intellij.openapi.projectRoots.Sdk
import com.intellij.openapi.roots.ContentEntry
import com.intellij.openapi.roots.ModifiableRootModel
import com.intellij.openapi.roots.OrderRootType
import com.intellij.psi.PsiJavaFile
import com.intellij.testFramework.LightProjectDescriptor
import com.intellij.testFramework.fixtures.DefaultLightProjectDescriptor
import com.intellij.testFramework.fixtures.LightJavaCodeInsightFixtureTestCase
import io.github.flinkpreview.analysis.FlinkPipelineAnalyzer
import java.io.File

/** Analyses the example job in examples/flink-job against the real Flink jars of one version. */
abstract class RealFlinkApiTest(private val descriptor: LightProjectDescriptor) : LightJavaCodeInsightFixtureTestCase() {

    override fun getProjectDescriptor() = descriptor

    fun testExampleJob() {
        val source = File("examples/flink-job/src/main/java/demo/FraudDetectionJob.java").readText()
        val file = myFixture.configureByText("FraudDetectionJob.java", source) as PsiJavaFile
        val result = FlinkPipelineAnalyzer.analyze(file, null)
        assertEquals("FraudDetectionJob.main()", result.scope)
        assertEquals(EXPECTED, result.graph!!.dump().trim())
    }

    class FlinkLibrary(private val property: String) : DefaultLightProjectDescriptor() {
        /** A real JDK, as in a real project, so java.lang types and generic inference resolve normally. */
        override fun getSdk(): Sdk = JavaSdk.getInstance().createJdk("test-jdk", System.getProperty("java.home"), false)

        override fun configureModule(module: Module, model: ModifiableRootModel, contentEntry: ContentEntry) {
            super.configureModule(module, model, contentEntry)
            val jars = System.getProperty(property)?.split(File.pathSeparator)?.filter { it.isNotBlank() }
                ?: error("System property $property is not set; run the tests through Gradle")
            val library = model.moduleLibraryTable.createLibrary("flink")
            val libraryModel = library.modifiableModel
            jars.forEach { libraryModel.addRoot("jar://$it!/", OrderRootType.CLASSES) }
            libraryModel.commit()
        }
    }

    companion object {
        val EXPECTED = """
            SOURCE fromData(Transaction, Transaction) name=Transactions
            OPERATOR assignTimestampsAndWatermarks(WatermarkStrategy)
            SOURCE fromData(Rule) name=Rules
            OPERATOR process(Screener) name=Screen uid=screen
            OPERATOR process(RuleEvaluator) name=Evaluate rules tags=[connect] p=4
            OPERATOR reduce(λ) name=Total per minute tags=[window(TumblingEventTimeWindows(1min))]
            SINK print() name=Totals
            SINK print() name=Alerts
            SINK print() name=Suspicious
            Transactions -> assignTimestampsAndWatermarks(WatermarkStrategy)
            assignTimestampsAndWatermarks(WatermarkStrategy) -> Screen
            Screen -> Evaluate rules PARTITION [keyBy(λ)]
            Rules -> Evaluate rules BROADCAST [broadcast]
            Screen -> Total per minute PARTITION [keyBy(λ)]
            Total per minute -> Totals
            Evaluate rules -> Alerts
            Screen -> Suspicious SIDE_OUTPUT [side output SUSPICIOUS, rebalance]
        """.trimIndent()
    }
}

private val FLINK_1X = RealFlinkApiTest.FlinkLibrary("flink.test.jars.1x")
private val FLINK_2X = RealFlinkApiTest.FlinkLibrary("flink.test.jars.2x")

class RealFlink1xApiTest : RealFlinkApiTest(FLINK_1X)
class RealFlink2xApiTest : RealFlinkApiTest(FLINK_2X)
