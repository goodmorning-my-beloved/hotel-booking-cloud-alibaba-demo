FROM apache/kafka:3.7.0

WORKDIR /app
ARG MODULE
COPY ${MODULE}/target/${MODULE}-*.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
