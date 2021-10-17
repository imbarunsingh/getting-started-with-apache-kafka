#Refer
https://www.baeldung.com/spring-cloud-stream-kafka-avro-confluent

#Pre-requisite :
##Start Kafka and Schema Registry
varunsingh@Varuns-MacBook-Pro confluent-kafka-6.2.0 % export CONFLUENT_HOME=/Users/varunsingh/Softwares/confluent-kafka-6.2.0

varunsingh@Varuns-MacBook-Pro confluent-kafka-6.2.0 % export PATH=$PATH:$CONFLUENT_HOME/bin 


confluent local start schema-registry

Access Confluent dashboard at :
http://localhost:9021/

confluent local services stop