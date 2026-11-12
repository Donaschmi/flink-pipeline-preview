plugins {
    java
}

repositories {
    mavenCentral()
}

tasks.withType<JavaCompile> {
    options.release = 17
}

// Switch to e.g. 2.3.0 with -PflinkVersion=2.3.0 to try the Flink 2.x API.
val flinkVersion = providers.gradleProperty("flinkVersion").getOrElse("1.20.5")

dependencies {
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
}
