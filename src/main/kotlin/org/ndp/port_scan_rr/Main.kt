package org.ndp.port_scan_rr

import org.ndp.port_scan_rr.utils.DatabaseHandler
import org.ndp.port_scan_rr.utils.KafkaHandler

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val results = KafkaHandler.consumeResult()
        DatabaseHandler.parsePortScanResult(results)
    }
}