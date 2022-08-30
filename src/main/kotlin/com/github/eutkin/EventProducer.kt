package com.github.eutkin

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import org.apache.ignite.Ignite
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

@Singleton
class EventProducer(
    @KafkaClient private val producer: Producer<String, String>,
    private val ignite: Ignite,
    @Property(name = "kafka.topic.name") private val topic: String,
) {

    companion object {
        private val log = LoggerFactory.getLogger(EventProducer::class.java)
    }

    private val affinity = this.ignite.affinity<String>(this.topic)

    fun send(key: String, value: String) : Long {
        val partition = this.affinity.partition(key)
        val producerRecord = ProducerRecord(this.topic, partition, key, value)
        val recordMetadata = this.producer.send(producerRecord).get()
        if (recordMetadata.hasOffset()) {
            log.info("Offset: {}", recordMetadata.offset())
            return recordMetadata.offset()
        }
        return 0
    }
}