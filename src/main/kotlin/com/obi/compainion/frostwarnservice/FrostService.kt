package com.obi.compainion.frostwarnservice

import com.beust.klaxon.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
class FrostService {

    val parser: Parser = Parser()
    val kafkaStreams: KafkaStreams = createStreamsInstance("localhost:9092")

    @PostConstruct
    fun init() {
        kafkaStreams.start()
    }

    @PreDestroy
    fun stop() {
        kafkaStreams.close()
    }


    private final fun createStreamsInstance(bootstrapServers: String): KafkaStreams {
        val config = Properties()
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "frost-application")
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        val builder = StreamsBuilder()
        val stream: KStream<String, String> = builder.stream("weather")
        stream.filter { key, value ->
            val json: JsonObject = parser.parse(StringBuilder(value)) as JsonObject
            "${json.string("celsius")}".toDouble() <= 3
        }.to("frost")
        return KafkaStreams(builder.build(), config)
    }
}


