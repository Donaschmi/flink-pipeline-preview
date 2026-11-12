import org.jetbrains.intellij.platform.gradle.TestFrameworkType

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.intellij.platform")
}

group = providers.gradleProperty("pluginGroup").get()
version = providers.gradleProperty("pluginVersion").get()

kotlin {
    jvmToolchain(21)
}

// Real Flink jars, used only by RealFlinkApiTest to check the analyzer against the actual APIs.
val flinkTest1x: Configuration by configurations.creating
val flinkTest2x: Configuration by configurations.creating

dependencies {
    flinkTest1x("org.apache.flink:flink-streaming-java:1.20.5")
    flinkTest2x("org.apache.flink:flink-streaming-java:2.3.0")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.opentest4j:opentest4j:1.3.0")

    intellijPlatform {
        // Use a locally installed IDE with -PplatformLocalPath=/path/to/idea, otherwise download platformVersion.
        val localPlatform = providers.gradleProperty("platformLocalPath")
        if (localPlatform.isPresent) local(localPlatform) else intellijIdea(providers.gradleProperty("platformVersion"))
        bundledPlugin("com.intellij.java")
        testFramework(TestFrameworkType.Platform)
        testFramework(TestFrameworkType.Plugin.Java)
    }
}

intellijPlatform {
    pluginConfiguration {
        ideaVersion {
            sinceBuild = providers.gradleProperty("pluginSinceBuild")
        }
    }
    buildSearchableOptions = false
}

tasks.test {
    val flink1x: FileCollection = flinkTest1x
    val flink2x: FileCollection = flinkTest2x
    inputs.files(flink1x, flink2x)
    jvmArgumentProviders += CommandLineArgumentProvider {
        listOf("-Dflink.test.jars.1x=${flink1x.asPath}", "-Dflink.test.jars.2x=${flink2x.asPath}")
    }
}
