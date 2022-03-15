# java-kafka-consumer
To run the script make sure that you have the compiled jar file "java-kafka-consumer-0.0.1.jar" in the same directory as 
the shell script "consumer.sh". In that same directory create a "conf" directory and add an "application-<env>.yaml" to 
that directory, where <env> is an arbitrary name you want to give the Kafka environment you are consuming from. See
the example YAML file, "application-dev.yaml" in this project's "conf" folder for an example of how to setup the file.
In that YAML, all properties under "consumer" are Kafka/Confluent consumer properties as defined 
[here](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html).

When running the script you specify the options like show below:

```
consumer.sh --topic mytopic --days 1 --timeout 5 --partitions 1 --env dev


   topic: the topic you want to produce to (required)
    
    days: the number of days backward to consume from - used only if the group exists already (default is 0)
    
    timeout: the number of seconds to wait without consuming a message before quitting (default is 5)
    
    log: the number of partitions to give the topic if created by this program (default is 1)
    
    env: the name of the environment (conf/application-<env>.yaml) to load
```