FROM openjdk:11.0.5-jre-stretch

ADD ["build/libs/port-scan-rr-1-all.jar", "settings.properties", "/"]

CMD java -jar port-scan-ns-1-all.jar