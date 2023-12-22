plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.vertx:vertx-core:4.5.1")
    implementation("io.vertx:vertx-codegen:4.5.1")
    implementation("com.ibm.etcd:etcd-java:0.0.24") {
        exclude(module = "gson")
    }
    implementation("org.furyio:fury-core:0.4.1")
    implementation("org.jetbrains:annotations:24.1.0")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.1")
}

tasks.withType(JavaCompile::class) {
    this.options.encoding = "utf-8"
}

tasks.test {
    useJUnitPlatform()
}
