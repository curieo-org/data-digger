FROM --platform=linux/amd64 maven:3.9.6 AS build

WORKDIR /usr/src/app

COPY data-digger-etl data-digger-etl
COPY data-mapper data-mapper
COPY pom.xml .

RUN mvn clean install -DskipTests
RUN cd data-digger-etl
RUN mvn package assembly:single -DskipTests
RUN cd ..

FROM --platform=linux/amd64 openjdk:21

WORKDIR /usr/src/app
COPY --from=build /usr/src/app .