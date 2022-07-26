FROM openjdk:8 as build_phase

COPY . /usr/src/myapp
WORKDIR /usr/src/myapp

RUN ./mvnw clean install

FROM openjdk:8 as run_phase
COPY --from=build_phase /usr/src/myapp/target/demo-0.0.1-SNAPSHOT.jar /demo-0.0.1-SNAPSHOT.jar
CMD ["sh", "run.sh"]
