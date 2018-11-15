/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch5x.client;

import java.io.IOException;
import java.net.*;
import java.util.*;
import org.apache.http.HttpHost;
import org.apache.skywalking.oap.server.library.client.*;
import org.apache.skywalking.oap.server.library.util.StringUtils;
import org.elasticsearch.action.admin.indices.create.*;
import org.elasticsearch.action.admin.indices.delete.*;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
public class ElasticSearchClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private static final String TYPE = "type";
    private final String clusterName;
    private final String clusterNodes;
    private final NameSpace namespace;
    private TransportClient client;

    public ElasticSearchClient(String clusterName, String clusterNodes, NameSpace namespace) {
        this.clusterName = clusterName;
        this.clusterNodes = clusterNodes;
        this.namespace = namespace;
    }

    @Override public void initialize() {
        List<HttpHost> pairsList = parseClusterNodes(clusterNodes);

        Settings settings = Settings.builder()
            .put("cluster.name", clusterName).build();

        client = new PreBuiltTransportClient(settings);

        pairsList.forEach(pairs -> {
            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(pairs.getHostName()), pairs.getPort()));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
    }

    @Override public void shutdown() {
        client.close();
    }

    private List<HttpHost> parseClusterNodes(String nodes) {
        List<HttpHost> httpHosts = new LinkedList<>();
        logger.info("elasticsearch cluster nodes: {}", nodes);
        String[] nodesSplit = nodes.split(",");
        for (String node : nodesSplit) {
            String host = node.split(":")[0];
            String port = node.split(":")[1];
            httpHosts.add(new HttpHost(host, Integer.valueOf(port)));
        }

        return httpHosts;
    }

    public boolean createIndex(String indexName, Settings settings,
        XContentBuilder mappingBuilder) throws IOException {
        indexName = formatIndexName(indexName);
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(settings);
        request.mapping(TYPE, mappingBuilder);

        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        logger.info("create {} index finished, isAcknowledged: {}", indexName, response.isAcknowledged());
        return response.isAcknowledged();
    }

    public boolean deleteIndex(String indexName) throws IOException {
        indexName = formatIndexName(indexName);
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        DeleteIndexResponse response;
        response = client.admin().indices().delete(request).actionGet();
        logger.info("delete {} index finished, isAcknowledged: {}", indexName, response.isAcknowledged());
        return response.isAcknowledged();
    }

    public boolean isExistsIndex(String indexName) throws IOException {
        indexName = formatIndexName(indexName);
        IndicesExistsRequest request = new IndicesExistsRequest();
        request.indices(indexName);
        return client.admin().indices().exists(request).actionGet().isExists();
    }

    public SearchResponse search(String indexName, SearchSourceBuilder searchSourceBuilder) throws IOException {
        indexName = formatIndexName(indexName);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(TYPE);
        searchRequest.source(searchSourceBuilder);

        return client.search(searchRequest).actionGet();
    }

    public GetResponse get(String indexName, String id) throws IOException {
        indexName = formatIndexName(indexName);
        GetRequest request = new GetRequest(indexName, TYPE, id);
        return client.get(request).actionGet();
    }

    public MultiGetResponse multiGet(String indexName, List<String> ids) throws IOException {
        final String newIndexName = formatIndexName(indexName);
        MultiGetRequest request = new MultiGetRequest();
        ids.forEach(id -> request.add(newIndexName, TYPE, id));
        return client.multiGet(request).actionGet();
    }

    public void forceInsert(String indexName, String id, XContentBuilder source) throws IOException {
        IndexRequest request = prepareInsert(indexName, id, source);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.index(request);
    }

    public void forceUpdate(String indexName, String id, XContentBuilder source, long version) throws IOException {
        indexName = formatIndexName(indexName);
        UpdateRequest request = prepareUpdate(indexName, id, source);
        request.version(version);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(request);
    }

    public void forceUpdate(String indexName, String id, XContentBuilder source) throws IOException {
        indexName = formatIndexName(indexName);
        UpdateRequest request = prepareUpdate(indexName, id, source);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(request);
    }

    public IndexRequest prepareInsert(String indexName, String id, XContentBuilder source) {
        indexName = formatIndexName(indexName);
        return new IndexRequest(indexName, TYPE, id).source(source);
    }

    public UpdateRequest prepareUpdate(String indexName, String id, XContentBuilder source) {
        indexName = formatIndexName(indexName);
        return new UpdateRequest(indexName, TYPE, id).doc(source);
    }

    public int delete(String indexName, String timeBucketColumnName, long endTimeBucket) throws IOException {
        indexName = formatIndexName(indexName);

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
            .filter(QueryBuilders.rangeQuery(timeBucketColumnName).lte(endTimeBucket))
            .source(indexName)
            .get();

        logger.debug("{} records delete from index {}", response.getDeleted(), indexName);
        return 0;
    }

    private String formatIndexName(String indexName) {
        if (Objects.nonNull(namespace) && StringUtils.isNotEmpty(namespace.getNameSpace())) {
            return namespace.getNameSpace() + "_" + indexName;
        }
        return indexName;
    }

    public BulkProcessor createBulkProcessor(int bulkActions, int bulkSize, int flushInterval,
        int concurrentRequests) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("{} data bulk failed, reason: {}", request.numberOfActions(), failure);
            }
        };

        return BulkProcessor.builder(client, listener)
            .setBulkActions(bulkActions)
            .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
            .setConcurrentRequests(concurrentRequests)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build();
    }
}
