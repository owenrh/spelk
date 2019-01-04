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
package com.ibm.spark.elk.metrics;

import com.codahale.metrics.*;
import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.util.Utils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchReporter extends ScheduledReporter {

	private final Logger log = LoggerFactory.getLogger(ElasticsearchReporter.class);

    private final String timeStampString = "YYYY-MM-dd'T'HH:mm:ss.SSSZ";

	private final Clock clock;
	private final String timestampField;

	private final SimpleDateFormat timestampFormat;

	private final String localhost;
	private String appName = null;
	private String appId = null;
	private String executorId = null;

	private RestHighLevelClient elastic;

	private final String indexName;
    private final DateFormat dateFormat;

	private ElasticsearchReporter(final MetricRegistry registry,
								  final MetricFilter filter,
								  final TimeUnit rateUnit,
								  final TimeUnit durationUnit,
								  final String host, // TODO: make hosts rather than host?
								  final String port,
								  final String indexName,
								  final String indexDateFormat,
								  final String timestampField) {

		super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);

		log.info("Creating metric system ElasticsearchReporter");

		this.clock = Clock.defaultClock();
		this.timestampField = timestampField;
		this.timestampFormat = new SimpleDateFormat(timeStampString);
		this.localhost = Utils.localHostName();

		this.indexName = indexName;
		if (indexDateFormat != null) {
            this.dateFormat = new SimpleDateFormat(indexDateFormat);
        }
        else {
		    this.dateFormat = null;
        }

        elastic = new RestHighLevelClient(RestClient.builder(new HttpHost(host, Integer.parseInt(port), "http")));
	}

	private String getIndexName() {
	    if (dateFormat != null) {
            return new StringBuilder()
                    .append(indexName)
                    .append("-")
                    .append(dateFormat.format(new Date())).toString();
        }
        else {
	        return indexName;
        }
    }

	@Override
	public void report(final SortedMap<String, Gauge> gauges,
                       final SortedMap<String, Counter> counters,
                       final SortedMap<String, Histogram> histograms,
                       final SortedMap<String, Meter> meters,
                       final SortedMap<String, Timer> timers) {

		final long timestamp = clock.getTime();
		String timestampString = timestampFormat.format(new Date(timestamp));

		if (appName == null) {
			SparkConf conf = SparkEnv.get().conf();
			appName = conf.get("spark.app.name", "");
			appId = conf.getAppId();
			executorId = conf.get("spark.executor,id", null);
		}

		try {
			final BulkRequest bulkRequest = new BulkRequest();

			if (!gauges.isEmpty()) {
				for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
					reportGauge(bulkRequest, entry, timestampString);
				}
			}

			if (!counters.isEmpty()) {
				for (Map.Entry<String, Counter> entry : counters.entrySet()) {
					reportCounter(bulkRequest, entry, timestampString);
				}
			}

			if (!histograms.isEmpty()) {
				for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
					reportHistogram(bulkRequest, entry, timestampString);
				}
			}

			if (!meters.isEmpty()) {
				for (Map.Entry<String, Meter> entry : meters.entrySet()) {
					reportMeter(bulkRequest, entry, timestampString);
				}
			}

			if (!timers.isEmpty()) {
				for (Map.Entry<String, Timer> entry : timers.entrySet()) {
					reportTimer(bulkRequest, entry, timestampString);
				}
			}

			final BulkResponse bulkResponse = elastic.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                log.error(bulkResponse.buildFailureMessage()); // TODO: tidy up
            }
		}
		catch (IOException e) {
			log.error("Exception posting to Elasticsearch index", e);
		}
	}

	private XContentBuilder buildMetricsDoc(final String name, final String timestampString) throws IOException {

        // ensure app and executor ids are set
        if (appId == null || executorId == null) {
            String nameParts[] = name.split("\\.");
            appId = nameParts[0];
            executorId = nameParts[1];
        }

        // extract metric name and replace dots
        String metricName = (name.substring(appId.length() + executorId.length() + 2)).replace('.', '_');

        // trim appName from start, if necessary
        if (metricName.startsWith(appName)) {
            metricName = metricName.substring(appName.length() + 1);
        }

        return jsonBuilder()
            .startObject()
            .field(timestampField, timestampString)
            .field("hostName", localhost)
            .field("applicationName", appName)
            .field("applicationId", appId)
            .field("executorId", executorId)
            .field("metricName", metricName);
    }

	private void reportGauge(final BulkRequest bulkRequest,
                             final Map.Entry<String, Gauge> entry,
                             final String timestampString) throws IOException {
        bulkRequest.add(new IndexRequest(getIndexName(), "_doc")
				.source(buildMetricsDoc(entry.getKey(), timestampString)
                .field("value", entry.getValue().getValue())
                .endObject()
            )
        );
    }

	private void reportCounter(final BulkRequest bulkRequest,
                               final Entry<String, Counter> entry,
                               final String timestampString) throws IOException {
        bulkRequest.add(new IndexRequest(getIndexName(), "_doc")
				.source(buildMetricsDoc(entry.getKey(), timestampString)
                .field("count", entry.getValue().getCount())
                .endObject()
            )
        );
	}

	private void reportHistogram(final BulkRequest bulkRequest,
                                 final Entry<String, Histogram> entry,
                                 final String timestampString) throws IOException {
        final Histogram histogram = entry.getValue();
        final Snapshot snapshot = histogram.getSnapshot();

        bulkRequest.add(new IndexRequest(getIndexName(), "_doc")
				.source(buildMetricsDoc(entry.getKey(), timestampString)
                        .field("min", convertDuration(snapshot.getMin()))
                        .field("max", convertDuration(snapshot.getMax()))
                        .field("mean", convertDuration(snapshot.getMean()))
                        .field("stddev", convertDuration(snapshot.getStdDev()))
                        .field("median", convertDuration(snapshot.getMedian()))
                        .field("75th percentile", convertDuration(snapshot.get75thPercentile()))
                        .field("95th percentile", convertDuration(snapshot.get95thPercentile()))
                        .field("98th percentile", convertDuration(snapshot.get98thPercentile()))
                        .field("99th percentile", convertDuration(snapshot.get99thPercentile()))
                        .field("999th percentile", convertDuration(snapshot.get999thPercentile()))
                        .endObject()
                )
        );
	}

	private void reportMeter(final BulkRequest bulkRequest,
                             final Entry<String, Meter> entry,
                             final String timestampString) throws IOException {
        final Meter meter = entry.getValue();

        bulkRequest.add(new IndexRequest(getIndexName(), "_doc")
				.source(buildMetricsDoc(entry.getKey(), timestampString)
                        .field("count", meter.getCount())
                        .field("mean rate", convertRate(meter.getMeanRate()))
                        .field("1-minute rate", convertRate(meter.getOneMinuteRate()))
                        .field("5-minute rate", convertRate(meter.getFiveMinuteRate()))
                        .field("15-minute rate", convertRate(meter.getFifteenMinuteRate()))
                        .endObject()
                )
        );
	}

	private void reportTimer(final BulkRequest bulkRequest,
                             final Entry<String, Timer> entry,
                             final String timestampString) throws IOException {
        final Timer timer = entry.getValue();
        final Snapshot snapshot = timer.getSnapshot();

        bulkRequest.add(new IndexRequest(getIndexName(), "_doc")
				.source(buildMetricsDoc(entry.getKey(), timestampString)
                        .field("count", timer.getCount())
                        .field("mean rate", convertRate(timer.getMeanRate()))
                        .field("1-minute rate", convertRate(timer.getOneMinuteRate()))
                        .field("5-minute rate", convertRate(timer.getFiveMinuteRate()))
                        .field("15-minute rate", convertRate(timer.getFifteenMinuteRate()))
                        .field("min", convertDuration(snapshot.getMin()))
                        .field("max", convertDuration(snapshot.getMax()))
                        .field("mean", convertDuration(snapshot.getMean()))
                        .field("stddev", convertDuration(snapshot.getStdDev()))
                        .field("median", convertDuration(snapshot.getMedian()))
                        .field("75th percentile", convertDuration(snapshot.get75thPercentile()))
                        .field("95th percentile", convertDuration(snapshot.get95thPercentile()))
                        .field("98th percentile", convertDuration(snapshot.get98thPercentile()))
                        .field("99th percentile", convertDuration(snapshot.get99thPercentile()))
                        .field("999th percentile", convertDuration(snapshot.get999thPercentile()))
                        .endObject()
                )
        );
	}

    @Override
    public void close() {
        super.close();
        try {
			elastic.close();
		}
		catch (IOException e) {
        	log.error("Exception closing Elastic client", e);
		}
    }

    public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	public static class Builder {
		private final MetricRegistry registry;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;

		private String host = "localhost";
		private String port = "9200";
		private String indexName = "spark-metrics";
		private String indexDateFormat;
		private String timestampField = "timestamp";

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		public Builder host(String hostName) {
			this.host = hostName;
			return this;
		}

		public Builder port(String port) {
			this.port = port;
			return this;
		}

		public Builder index(String index) {
			this.indexName = index;
			return this;
		}

        public Builder indexDateFormat(String dateFormat) {
            this.indexDateFormat = dateFormat;
            return this;
        }

		public Builder timestampField(String timestampFieldName) {
			this.timestampField = timestampFieldName;
			return this;
		}

		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		public ElasticsearchReporter build() {
			return new ElasticsearchReporter(registry, filter, rateUnit, durationUnit, host, port, indexName, indexDateFormat, timestampField);
		}
	}
}
