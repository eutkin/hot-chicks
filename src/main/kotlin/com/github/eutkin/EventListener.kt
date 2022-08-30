package com.github.eutkin

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.messaging.annotation.MessageBody
import jakarta.inject.Singleton
import org.apache.ignite.Ignite

@Singleton
class EventListener(private val ignite: Ignite) : KafkaListenerExceptionHandler {

    @KafkaListener(
        offsetReset = OffsetReset.LATEST,
        groupId = "event",
        uniqueGroupId = true,
        threads = 2
    )
    @Topic("\${kafka.topic.name}")
    fun listen(@KafkaKey key: String, @MessageBody value: String) {
        ignite.cache<String, String>("topic1").put(key, value)
        println(ignite.affinity<String>("topic1").partition(key))
    }

    override fun handle(exception: KafkaListenerException?) {
        TODO("Not yet implemented")
    }
}