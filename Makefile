ENV ?= .env
-include $(ENV)
export

producer:
	@set -a; . $(ENV); set +a; \
	python3 Producers/prevalence_producer.py

consumer:
	@set -a; . $(ENV); set +a; \
	python3 Consumers/prevalence_consumer.py

visual:
	@set -a; . $(ENV); set +a; \
	python3 Consumers/prevalence_consumer_visual.py

topic-create:
	@JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
	kafka-topics --create --topic $(TOPIC) \
	--bootstrap-server $(KAFKA_BOOTSTRAP) --partitions 1 --replication-factor 1 || true
