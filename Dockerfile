FROM debian:stable

RUN apt-get update && \
    apt-get install -y libarray-diff-perl gnupg openjdk-11-jre

ADD snpcheck_compare_vcfs snpcheck_compare_vcfs
RUN chmod a+x snpcheck_compare_vcfs
ADD target/lib /usr/share/hartwig/lib
ADD target/snpcheck-local-SNAPSHOT.jar /usr/share/hartwig/snpcheck.jar

CMD ["java", "-jar", "/usr/share/hartwig/snpcheck.jar"]
