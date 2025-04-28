FROM eclipse-temurin:11-jre

ADD target/lib /usr/share/hartwig/lib
ADD target/snpcheck-local-SNAPSHOT.jar /usr/share/hartwig/snpcheck.jar

ENTRYPOINT ["java", "-jar", "/usr/share/hartwig/snpcheck.jar"]
