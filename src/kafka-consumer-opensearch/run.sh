#!/bin/bash


build() {

  ## 
  echo "### Maven build"
  mvn clean package
}

pd() { 
  echo "### Producing.."
  java -cp target/kafka-producer-wikimedia-1.0-SNAPSHOT.jar kafka.wikimedia.WikimediaChangesProducer
}

cm() {
  echo "### Consuming.."
  java -cp target/kafka-consumer-opensearch-1.0-SNAPSHOT.jar kafka.opensearch.OpenSearchConsumer
}


case $1 in
  build)
    build
    ;;
  pd)
    pd
    ;;
  cm)
    cm
    ;;
  *)
    echo "Usage: $0 {build, pd, cm}"
    ;;
esac