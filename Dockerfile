FROM openjdk:17-alpine

WORKDIR code
ADD deploy/server/* ./

ENV JAVA_OPTS "-Dlog4j.configurationFile=log4j2.xml -Djava.net.preferIPv4Stack=true -DlogFilename=logs"

ENTRYPOINT ["/bin/sh"]

