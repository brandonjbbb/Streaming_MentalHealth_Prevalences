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
.PHONY: help install env check-env producer consumer visual topic-create topic-list topic-describe verify-topic

help:
	@echo "make install        # pip install -r requirements.txt"
	@echo "make env            # print resolved env vars"
	@echo "make topic-create   # create Kafka topic (idempotent)"
	@echo "make verify-topic   # confirm topic exists"
	@echo "make producer       # run CSV -> Kafka producer"
	@echo "make consumer       # run simple consumer"
	@echo "make visual         # run animated consumer"
	@echo "make topic-list     # list all topics"
	@echo "make topic-describe # describe current TOPIC"

install:
	pip3 install -r requirements.txt

env:
	@set -a; . $(ENV); set +a; \
	echo "KAFKA_BOOTSTRAP=$$KAFKA_BOOTSTRAP  TOPIC=$$TOPIC"

check-env:
	@test -f $(ENV) || { echo "Missing $(ENV). Try: cp .env.example .env"; exit 1; }

producer: check-env
	@set -a; . $(ENV); set +a; \
	python3 Producers/prevalence_producer.py

consumer: check-env
	@set -a; . $(ENV); set +a; \
	python3 Consumers/prevalence_consumer.py

visual: check-env
	@set -a; . $(ENV); set +a; \
	python3 Consumers/prevalence_consumer_visual.py

topic-create: check-env
	@set -a; . $(ENV); set +a; \
	JAVA_HOME=/opt/homebrew/opt/openjdk@17 kafka-topics --create --topic $$TOPIC \
	--bootstrap-server $$KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1 || true

verify-topic:
	@set -a; . $(ENV); set +a; \
	kafka-topics --list --bootstrap-server $$KAFKA_BOOTSTRAP | grep -n "$$TOPIC" || true

topic-list:
	@set -a; . $(ENV); set +a; \
	kafka-topics --list --bootstrap-server $$KAFKA_BOOTSTRAP

topic-describe:
	@set -a; . $(ENV); set +a; \
	kafka-topics --describe --topic $$TOPIC --bootstrap-server $$KAFKA_BOOTSTRAP || true
