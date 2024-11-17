plugins {
    id("java")
    idea
}

group = "org.atlantfs"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    val main by getting
    val test by getting
    register("functionalTest") {
        compileClasspath += main.output
        compileClasspath += test.output
        runtimeClasspath += main.output
        runtimeClasspath += test.output
    }
}

idea {
    module {
        testSources.from(sourceSets.named("functionalTest").get().allSource.srcDirs)
    }
}

configurations {
    named("functionalTestImplementation") {
        extendsFrom(getByName("testImplementation"))
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.mockito:mockito-core:3.+")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.3")
}

tasks {
    test {
        useJUnitPlatform()
    }
    val functionalTest by registering(Test::class) {
        group = "verification"
        useJUnitPlatform()
        val functionalTest = sourceSets.getByName("functionalTest")
        testClassesDirs = functionalTest.output.classesDirs
        classpath = functionalTest.runtimeClasspath
        shouldRunAfter("test")
    }
    getByName("check").dependsOn(functionalTest)
    withType<Test> {
        systemProperty(
            "java.util.logging.config.file",
            project.layout.projectDirectory.file("logging.properties").asFile.absolutePath
        )
        systemProperty("project.dir", project.layout.projectDirectory.asFile.absolutePath)
        systemProperty("build.dir", project.layout.buildDirectory.get().asFile.absolutePath)
        systemProperty("atlant.dir", project.layout.buildDirectory.dir("atlant").get().asFile.absolutePath)
    }
}
