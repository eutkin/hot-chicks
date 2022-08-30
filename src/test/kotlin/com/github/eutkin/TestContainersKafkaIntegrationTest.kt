package com.github.eutkin

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.affinity.AffinityKeyMapper
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin.NewTopic
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.TimeUnit

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("На стендах не настроено тестирование через докер")
class TestContainersKafkaIntegrationTest : TestPropertyProvider {

    private companion object {
        private const val topicName = "topic1"
        private val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))

        init {
            kafka.start()
            val adminClient = AdminClient.create(mapOf(BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers))
            val createTopics = adminClient.createTopics(listOf(NewTopic(topicName, 10, 1)))
            createTopics.all().get()
            adminClient.close(Duration.ofSeconds(10))
        }

        @AfterAll
        @JvmStatic
        fun close() {
            kafka.stop()
        }
    }

    @Inject
    lateinit var ignite : Ignite

    @Inject
    lateinit var listener: EventListener

    @Inject
    lateinit var client: EventProducer


    @Test
    fun testItWorks() {
       ignite.createCache(
            CacheConfiguration<String, String>(topicName)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAffinity(RendezvousAffinityFunction(10, null))
                .setAffinityMapper(object : AffinityKeyMapper {
                    override fun affinityKey(key: Any): Any {
                        val (_, aff) = key.toString().split("|")
                        return aff
                    }

                    override fun reset() {

                    }

                })
        )
        client.send("Hi|Hi", "hi")
        client.send("Hi|Hi", "hi")
        client.send("Hi|Hi", "hi")

        listener.latch.await(5, TimeUnit.SECONDS)

    }


    override fun getProperties(): MutableMap<String, String> = mutableMapOf(
        "kafka.bootstrap.servers" to "localhost:${kafka.firstMappedPort}",
        "kafka.topic.name" to topicName,
        "kafka.enabled" to "true",
    )

}