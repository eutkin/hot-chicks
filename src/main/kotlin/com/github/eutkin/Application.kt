package com.github.eutkin

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.runtime.Micronaut.*

fun main(args: Array<String>) {
    build()
        .args(*args)
        .packages("com.github.eutkin")
        .eagerInitSingletons(true)
        .eagerInitAnnotated(KafkaClient::class.java, KafkaListener::class.java)
        .start()
}

