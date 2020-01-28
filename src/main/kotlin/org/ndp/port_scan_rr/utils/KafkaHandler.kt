package org.ndp.port_scan_rr.utils

import com.squareup.moshi.JsonAdapter
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.ndp.port_scan_rr.bean.KafkaResult
import org.ndp.port_scan_rr.utils.Logger.logger
import java.time.Duration
import java.util.*

object KafkaHandler {
    private val consumer: KafkaConsumer<String, String>
    private val kafkaResultAdapter: JsonAdapter<KafkaResult>

    init {

        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] =
            Settings.setting["BOOTSTRAP_SERVERS_CONFIG"] as String
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] =
            Settings.setting["GROUP_ID_CONFIG"] as String
        consumerProps[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] =
            Settings.setting["SESSION_TIMEOUT_MS_CONFIG"] as String
        consumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] =
            Settings.setting["MAX_POLL_RECORDS_CONFIG"] as String
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            Settings.setting["KEY_DESERIALIZER_CLASS_CONFIG"] as String
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            Settings.setting["VALUE_DESERIALIZER_CLASS_CONFIG"] as String
        consumer = KafkaConsumer(consumerProps)
        consumer.subscribe(arrayListOf(Settings.setting["topic.task"] as String))

        val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
        kafkaResultAdapter = moshi.adapter(KafkaResult::class.java)
    }

    fun consumeResult(): List<KafkaResult> {
        logger.debug("start kafka consumer...")
        val msgList = consumer.poll(Duration.ofSeconds(10))
        logger.debug("consumed results: ${msgList.count()}")
        val results = ArrayList<KafkaResult>()
        if (!msgList.isEmpty) {
            for (record in msgList.records(Settings.setting["topic.task"] as String)) {
                results.add(kafkaResultAdapter.fromJson(record.value().toString())!!)
            }
        }
        return results
    }
}