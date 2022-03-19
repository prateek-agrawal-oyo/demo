FROM openjdk:8

COPY . /usr/src/myapp
WORKDIR /usr/src/myapp

RUN ./mvnw clean install

CMD ["sh", "run.sh"]
