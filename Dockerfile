FROM openjdk:8

COPY . /usr/src/myapp
WORKDIR /usr/src/myapp

RUN ./mvnw clean install

CMD ["java", "-jar", "target/demo-0.0.1-SNAPSHOT.jar"]
