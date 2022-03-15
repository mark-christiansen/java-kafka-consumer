#!/bin/bash

program_name="java-kafka-consumer"
version="0.0.1"

display_help() {
    echo "Usage: $0 [option...]" >&2
    echo ""
    echo "   -h, --help                 Displays script usage info"
    echo "   -t, --topic                The topic to consume from"
    echo "   -d, --days                 The number of days to go backwards to consume from on the topic"
    echo "   -p, --timeout              The timeout (in seconds) that must be reached without consuming a message before quitting"
    echo "   -l, --log                  Boolean (true/false) indicating whether to log messages to the system console"
    echo "   -e, --env                  The Kafka environment to connect to (application-<env>.yaml)"
    echo ""
    exit 1
}

topic=""
days="0"
timeout=""
log=""
env=""

# find argument names and values passed into the program
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help) display_help; shift ;;
        -t|--topic) topic="$2"; shift ;;
        -d|--days) days="$2"; shift ;;
        -p|--timeout) timeout="$2"; shift ;;
        -l|--log) log="$2"; shift ;;
        -e|--env) env="$2"; shift ;;
        *) echo "Unknown parameter passed: $1" ;;
    esac
    shift
done

# verify required arguments were passed
[[ $topic == "" ]] && { echo "The option --topic (-t) is required but not set, please specify the topic name" && display_help && exit 1; }
[[ $env == "" ]] && { echo "The option --env (-e) is required but not set, please specify the environment name"  && display_help && exit 1; }
topic="--topic $topic"
days="--days $days"
if [[ $timeout != "" ]]; then
  timeout="--timeout $timeout"
fi
if [[ $log != "" ]]; then
  log="--log $log"
fi

# execute program
java -cp "$program_name-$version.jar" -Dloader.main=com.jnj.kafka.consumer.Application -Dspring.profiles.active=$env -Dspring.config.location=conf/ org.springframework.boot.loader.PropertiesLauncher $topic $days $timeout $log