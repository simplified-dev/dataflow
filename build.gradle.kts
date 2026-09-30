plugins {
    id("java-library")
}

group = "dev.sbs"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    // JetBrains Annotations
    api(libs.annotations)

    // Gson (pipeline definition serde)
    api(libs.gson)

    // Jackson XML (XML -> JsonElement source bridge)
    api(libs.jackson.dataformat.xml)

    // Jsoup (HTML parsing + CSS selectors)
    api(libs.jsoup)

    // SLF4J (logging)
    api(libs.slf4j.api)

    // Simplified Libraries (extracted to github.com/simplified-dev)
    api("com.github.simplified-dev:client") { version { strictly("b810558") } }
    api("com.github.simplified-dev:collections") { version { strictly("4029e80") } }
    api("com.github.simplified-dev:gson-extras") { version { strictly("3ac0d4f") } }
    api("com.github.simplified-dev:reflection") { version { strictly("5186e88") } }

    // Simplified Annotations
    compileOnly(libs.simplified.annotations)
    annotationProcessor(libs.simplified.annotations)
    testCompileOnly(libs.simplified.annotations)
    testAnnotationProcessor(libs.simplified.annotations)

    // Tests
    testImplementation(libs.hamcrest)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.junit.platform.launcher)
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-parameters")
}

tasks.test {
    useJUnitPlatform()
}
