FROM openjdk:8-jdk-slim

WORKDIR /app

COPY gradlew /app/gradlew
COPY gradle /app/gradle
COPY build.gradle /app/build.gradle
COPY settings.gradle /app/settings.gradle
COPY gradle.properties /app/gradle.properties
COPY config/ /app/config
COPY algorithm/ /app/algorithm
COPY run_tests.sh /app/run_tests.sh

RUN mkdir /app/stream-profiling
RUN mkdir /app/stream-profiling-data
RUN mkdir /app/stream-profiling/graphs

RUN apt-get update \
    && apt-get install -y python3 python3-pip wget \
      dvipng ghostscript texlive-fonts-recommended texlive-latex-base texlive-latex-extra \
      texlive-latex-recommended texlive-publishers texlive-science texlive-xetex cm-super gcc libpq-dev \
    && apt-get clean

# download the datasets from the website and store them on the folder /app/stream-profiling
RUN wget -r -np -nH --cut-dirs=1 --reject "index.html*" \
        https://big.csr.unibo.it/downloads/stream-profiling/ -P /app && \
    apt-get remove -y wget && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN mv /app/stream-profiling/* /app/stream-profiling-data
# install the requirements
RUN pip3 install -r algorithm/src/main/python/requirements.txt
# append to the file algorithm/src/main/resources/application.conf native_python: true and before add a new line
RUN echo ", native_python = true" >> algorithm/src/main/resources/application.conf
RUN mkdir tmp

RUN chmod +x gradlew
RUN ./gradlew algorithm:shadowJar --no-daemon -Dorg.gradle.daemon=false -Dorg.gradle.vfs.watch=false

RUN chmod +x /app/run_tests.sh

CMD ["bash", "/app/run_tests.sh"]