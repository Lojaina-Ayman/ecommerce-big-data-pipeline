FROM eclipse-temurin:17-jdk AS build
ARG SBT_VERSION=1.12.8
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar -xz -C /opt
ENV PATH="/opt/sbt/bin:${PATH}"
WORKDIR /app
COPY build.sbt ./
COPY project/ project/
RUN sbt update
COPY src/ src/
RUN sbt package

FROM scratch AS export
COPY --from=build /app/target/scala-2.13/ecommerce-pipeline_2.13-*.jar /ecommerce-pipeline.jar
