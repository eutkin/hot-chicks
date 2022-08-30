package com.github.eutkin

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.messaging.annotation.MessageBody
import jakarta.inject.Singleton
import org.apache.ignite.Ignite
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch

@Singleton
class EventListener(private val ignite: Ignite) : KafkaListenerExceptionHandler {

    companion object {
        private val log = LoggerFactory.getLogger(EventListener::class.java)
    }

    val latch = CountDownLatch(3)


    @KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "event",
        uniqueGroupId = true,
        offsetStrategy = OffsetStrategy.AUTO
    )
    @Topic("\${kafka.topic.name}")
    fun listen(@KafkaKey key: String, @MessageBody value: String) {
        log.info("Consumer: {} - {}", key, value)
        ignite.cache<String, String>("topic1").put(key, value)
        log.info(ignite.affinity<String>("topic1").partition(key).toString())
        this.latch.countDown()
    }

    override fun handle(exception: KafkaListenerException) {
        log.error(exception.message, exception)
        latch.countDown()
    }
}