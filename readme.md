Run kafka and zookeeper and create a kafka server and group: .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group vvs_rt_data 
On the terminal run zookeeper : .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
and start the kafka server: bin\windows\kafka-server-start.bat config\server.properties

activate the virtual environment: run myenv\Scripts\activate
In the virtual envinronment, start the consumer: python vvs_consumer.py
and producer: python vvs_zone_1_producer.py

make sure vvs_steige.csv and current_time.json files are in the same folder as the scripts.