/*
 * Copyright 2016 IBM Corp.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.elk.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.ibm.spark.elk.metrics.ElasticsearchReporter
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.SecurityManager
import org.slf4j.LoggerFactory

class ElasticsearchSink(properties: Properties, registry: MetricRegistry, securityMgr: SecurityManager) extends Sink {
  @transient lazy private val log = LoggerFactory.getLogger(getClass.getName)

  // configuration keys
  object Keys {
    val host = "host"
    val port = "port"
    val indexName = "index"
    val indexDateFormat = "indexDateFormat"
    val pollPeriod = "period"
    val pollPeriodUnit = "unit"
  }

  // configuration defaults
  object Defaults {
    val defaultPeriod = 10
    val defaultUnit = "SECONDS"
    val defaultIndexName = "spark-metrics"
  }

  // host and port must be specified

  def message(msg: String) = s"'$msg' not specified for Elasticsearch sink"

  val host = properties.getProperty(Keys.host)
  if (host == null) {
    log.error(message("host"))
    throw new Exception(message("host"))
  }

  val port = properties.getProperty(Keys.port)
  if (port == null) {
    log.error(message("port"))
    throw new Exception(message("port"))
  }

  val index = properties.getProperty(Keys.indexName, Defaults.defaultIndexName)
  val indexDateFormat = properties.getProperty(Keys.indexDateFormat)

  val pollPeriod = Option(properties.getProperty(Keys.pollPeriod)) match {
    case Some(s) => s.toInt
    case None => Defaults.defaultPeriod
  }

  val pollUnit: TimeUnit = Option(properties.getProperty(Keys.pollPeriodUnit)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(Defaults.defaultUnit)
  }

  val reporter: ElasticsearchReporter = ElasticsearchReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .host(host)
    .port(port)
    .index(index)
    .indexDateFormat(indexDateFormat)
    .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
