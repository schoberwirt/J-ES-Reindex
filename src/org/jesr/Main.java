package org.jesr;


import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.search.SearchType.COUNT;
import static org.elasticsearch.action.search.SearchType.SCAN;

/**
 * Created by abonuccelli on 15/02/15.
 */
public class Main {

    private static boolean DEBUG = false;

    private final static String LINE = "---------------------------------";

    //ARGS NAMES
    //source
    private final static String ARGS_SRC_HOST = "--shost";
    private final static String ARGS_SRC_PORT = "--sport";
    private final static String ARGS_SRC_CLUSTER_NAME = "--sclsname";
    private final static String ARGS_SRC_INDEX = "--sidx";
    //destination
    private final static String ARGS_DST_HOST = "--dhost";
    private final static String ARGS_DST_PORT = "--dport";
    private final static String ARGS_DST_CLUSTER_NAME = "--dclsname";
    private final static String ARGS_DST_INDEX = "--didx";
    private final static String ARGS_USE_SAME_CLS = "--usesamecls";
    //config
    private final static String ARGS_CFG_SCROLL_SIZE = "--scrollsize";
    private final static String ARGS_CFG_BULK_SIZE = "--bulksize";
    private final static String ARGS_CFG_BULK_CONC_REQ = "--bulkconcreq";
    private final static String ARGS_CFG_SNIFF = "--sniff";
    private final static String ARGS_CFG_BULK_QUEUE_WARN_TH = "--bulkqwth";
    //auth
    private final static String ARGS_AUTH_SUSER = "--suser";
    private final static String ARGS_AUTH_SPASS = "--spass";
    private final static String ARGS_AUTH_DUSER = "--duser";
    private final static String ARGS_AUTH_DPASS = "--dpass";


    //DEFAULT PARAMS
    private static int srcPort = 9300;
    private static String srcHost = "localhost";
    private static String srcClusterName = "elasticsearch";
    private static String srcIndex = "";

    private static int dstPort = 9300;
    private static String dstHost = "localhost";
    private static String dstClusterName = "elasticsearch";
    private static String dstIndex = "";

    private static String srcClsUser = "";
    private static String srcClsPass = "";
    private static String dstClsUser = "";
    private static String dstClsPass = "";

    private static int cfgScrollSize = 200;
    private static int cfgBulkSize = 1000;
    private static int cfgBulkConcReq = 5;
    private static boolean cfgSniff = true;


    //RUNTIME VALUES
    private static Client srcClient = null;
    private static Client dstClient = null;
    private static boolean useSameCls = false;
    private static long srcIdxDocCount = -1;
    private static long dstIdxDocCount = -1;
    private static int bulkQWarnThreshold = 10;
    private static long sleepOnBulkRejected = 10000;



    public static void main(String[] args) {

        parseArgs(args);
        if (DEBUG) printParams();
        srcClient = initClient(srcHost, srcPort, srcClusterName, srcClsUser, srcClsPass, cfgSniff);
        if (useSameCls) {
            dstClient = initClient(srcHost, srcPort, srcClusterName, srcClsUser, srcClsPass, cfgSniff);
        } else {
            dstClient = initClient(dstHost, dstPort, dstClusterName, dstClsUser, dstClsPass, cfgSniff);
        }
        srcIdxDocCount = countIndexDocs(srcClient, srcIndex);
        System.out.println("Reindexing " + srcIdxDocCount + " documents from " +
                srcHost + ":" + srcPort + "//" + srcClusterName + "/" + srcIndex + " to " +
                dstHost + ":" + dstPort + "//" + dstClusterName + "/" + dstIndex);
        reIndex(srcClient, dstClient, matchAllQuery(), srcIndex, dstIndex);
        closeClient(srcClient);
        closeClient(dstClient);

    }


    private static void printUsage(){
        System.out.println(LINE);
        System.out.println("Elasticsearch Java Reindex tool v0.5");
        System.out.println(LINE);
        System.out.println("Usage: java -jar es-java-cli.jar [OPTIONS]");
        System.out.println(LINE);
        System.out.println("Generic Options:");
        String options = ARGS_SRC_HOST + " <source_host> [default localhost]\n" +
                ARGS_SRC_PORT + " <source_port> [default 9300]\n" +
                ARGS_SRC_CLUSTER_NAME + " <source_cluster_name> [default elasticsearch]\n" +
                ARGS_SRC_INDEX + " <source_index_name>\n" +
                ARGS_DST_HOST + " <destination_host> [default localhost]\n" +
                ARGS_DST_PORT + " <destination_port> [default 9300]\n" +
                ARGS_DST_CLUSTER_NAME + " <destination_cluster_name> [default elasticsearch]\n" +
                ARGS_DST_INDEX + " <destination_index_name>\n" +
                ARGS_USE_SAME_CLS + " <use_src_cluster_params(host/port/clsname)_for_dst_cluster> [default false]";
        System.out.println(options);
        System.out.println(LINE);
        System.out.println("Scroll/Bulk Options:");
        System.out.println(
                ARGS_CFG_SCROLL_SIZE + " <scroll_size>  [default 200]\n" +
                        ARGS_CFG_BULK_SIZE + " <bulk_doc_size>  [default 1000]\n" +
                        ARGS_CFG_BULK_CONC_REQ + " <bulk_concurrent_requests> [default 5]\n" +
                        ARGS_CFG_SNIFF + " <use_sniffing> [default true]\n" +
                        ARGS_CFG_BULK_QUEUE_WARN_TH + " <when_bulk_threadpool_q_passes_this_threshold_decrease_bulk_size_by_20%> [default " +bulkQWarnThreshold +"]");
        System.out.println(LINE);
        System.out.println("Shield options:");
        options = ARGS_AUTH_SUSER + " <source_cluster_username>\n" +
                ARGS_AUTH_SPASS + " <source_cluster_password>\n" +
                ARGS_AUTH_DUSER + " <destination_cluster_username>\n" +
                ARGS_AUTH_DPASS + " <destination_cluster_password>\n";
        System.out.println(options);
        System.exit(1);
    }

    private static void parseArgs(String[] args) {
        if ((args.length == 0) || (args.length % 2 != 0)) {
            printUsage();
            if (DEBUG) printParams();
        }


        try {
            for (int i = 0; i < args.length; i += 2) {
                if (!args[i].startsWith("--")) {
                    throw new Exception("Invalid parameter " + args[i]);
                }
                switch (args[i]) {
                    case ARGS_SRC_HOST:
                        srcHost = args[i + 1];
                        break;
                    case ARGS_SRC_PORT:
                        srcPort = Integer.valueOf(args[i + 1]);
                        break;
                    case ARGS_SRC_INDEX:
                        srcIndex = args[i + 1];
                        break;
                    case ARGS_SRC_CLUSTER_NAME:
                        srcClusterName = args[i + 1];
                        break;
                    case ARGS_DST_HOST:
                        dstHost = args[i + 1];
                        break;
                    case ARGS_DST_PORT:
                        dstPort = Integer.valueOf(args[i + 1]);
                        break;
                    case ARGS_DST_INDEX:
                        dstIndex = args[i + 1];
                        break;
                    case ARGS_DST_CLUSTER_NAME:
                        dstClusterName = args[i + 1];
                        break;
                    case ARGS_CFG_SCROLL_SIZE:
                        cfgScrollSize = Integer.valueOf(args[i + 1]);
                        break;
                    case ARGS_CFG_BULK_CONC_REQ:
                        cfgBulkConcReq = Integer.valueOf(args[i + 1]);
                        break;
                    case ARGS_CFG_BULK_SIZE:
                        cfgBulkSize = Integer.valueOf(args[i + 1]);
                        break;
                    case ARGS_CFG_SNIFF:
                        cfgSniff = Boolean.valueOf(args[i + 1]);
                        break;
                    case ARGS_AUTH_SUSER:
                        srcClsUser = args[i + 1];
                        break;
                    case ARGS_AUTH_SPASS:
                        srcClsPass = args[i + 1];
                        break;
                    case ARGS_AUTH_DUSER:
                        dstClsUser = args[i + 1];
                        break;
                    case ARGS_AUTH_DPASS:
                        dstClsPass = args[i + 1];
                        break;
                    case ARGS_USE_SAME_CLS:
                        useSameCls = Boolean.valueOf(args[i + 1]);
                        break;
                    case ARGS_CFG_BULK_QUEUE_WARN_TH:
                        bulkQWarnThreshold = Integer.valueOf(args[i + 1]);
                        break;
                }
            }
        } catch (Exception e) {

        }
    }

    public static void printParams() {
        System.out.println("#### SOURCE ####");
        System.out.println("SRC HOST -> " + srcHost);
        System.out.println("SRC PORT -> " + srcPort);
        System.out.println("SRC CNAME -> " + srcClusterName);
        System.out.println("SRC IDX -> " + srcIndex);
        System.out.println("#### DESTINATION ####");
        System.out.println("DST HOST -> " + dstHost);
        System.out.println("DST PORT -> " + dstPort);
        System.out.println("DST CNAME -> " + dstClusterName);
        System.out.println("DST IDX -> " + dstIndex);
        System.out.println("USE SAME CLS -> " + useSameCls);
        System.out.println("#### CONFIG ####");
        System.out.println("CFG SCROLL SIZE -> " + cfgScrollSize + " [default 200]");
        System.out.println("CFG BULK SIZE -> " + cfgBulkSize + " [default 1000]");
        System.out.println("CFG BULK CONCUR REQ -> " + cfgBulkConcReq + " [default 5]");
        System.out.println("CFG SNIFF -> " + cfgSniff + " [default true]");
        System.out.println("#### AUTH ####");
        System.out.println("SRC USERNAME -> " + srcClsUser);
        System.out.println("SRC PASSWORD -> " + srcClsPass);
        System.out.println("DST USERNAME -> " + dstClsUser);
        System.out.println("DST PASSWORD -> " + dstClsPass);
    }


    public static Client initClient(String host, int port, String clustername, String userName, String password, boolean sniff) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clustername)
                .put("client.transport.sniff", sniff)
                .put("shield.user", userName + ":" + password)
                .build();

        return new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(host, port));
    }

    public static void closeClient(Client client) {
        if (client != null) {
            client.close();
        }
    }

    private static QueryBuilder matchAllQuery() {
        return QueryBuilders.matchAllQuery();
    }


    public static void reIndex(Client srcClient, Client dstClient, QueryBuilder qb, String srcIndex, String dstIndex) {


        BulkProcessor bulkProcessor = BulkProcessor.builder(dstClient,
                createLoggingBulkProcessorListener()).setBulkActions(cfgBulkSize)
                .setConcurrentRequests(cfgBulkConcReq)
                .setFlushInterval(TimeValue.timeValueSeconds(1000))
                .build();


        try {
            SearchResponse scrollResp = srcClient.prepareSearch(srcIndex)
                    .setSearchType(SCAN)
                    .setScroll(new TimeValue(60000))
                    .setQuery(matchAllQuery().buildAsBytes())
                    .setSize(cfgScrollSize).execute().actionGet();

            scrollResp = srcClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();


            while (true) {
                if (scrollResp.getHits().getHits().length == 0) {
                    bulkProcessor.awaitClose(30000, TimeUnit.MILLISECONDS);
                    break;
                }
                for (SearchHit hit : scrollResp.getHits()) {

                    bulkProcessor.add(Requests.indexRequest(dstIndex)
                                    .id(hit.getId())
                                    .type(hit.getType())
                                    .source(hit.getSourceAsString())
                    );
                }

                scrollResp = srcClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                ActionFuture<NodesStatsResponse> nodesStatsResponse = getStats(dstClient);
                double avgBulkQueueSize = getBulkQueueAvg(nodesStatsResponse);
                boolean bulkRejects = isBulkRejecting(nodesStatsResponse);
                int cfgBulkSizeDecrease;
                if (bulkRejects) {
                    System.out.println("WARN: bulk thread rejected...sleeping 10 seconds");
                    Thread.sleep(sleepOnBulkRejected);
                    System.out.println("WARN: resuming operations");
                }
                if (avgBulkQueueSize > bulkQWarnThreshold){
                    System.out.println("WARN: detected non-null average bulk queue size " + avgBulkQueueSize);
                    cfgBulkSizeDecrease = cfgBulkSize / 5;
                    cfgBulkSize = cfgBulkSize - cfgBulkSizeDecrease;
                    System.out.println("WARN: decreasing bulk-size to " + cfgBulkSize);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ActionFuture<NodesStatsResponse> getStats(Client client) {
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest().clear().threadPool(true);
        ActionFuture<NodesStatsResponse> nodesStatsResponse = client.admin().cluster().nodesStats(nodesStatsRequest);
        return nodesStatsResponse;
    }

    private static double getBulkQueueAvg(ActionFuture<NodesStatsResponse> nodesStatsResponse){
        //returns average bulk queue size across nodes;
        double result = 0;
        int howManyNodes = 1;
        JSONParser jsonParser = new JSONParser();
        String rawJSON = nodesStatsResponse.actionGet().toString();
        try {
            JSONObject rootObject = (JSONObject) jsonParser.parse(rawJSON);
            JSONObject nodes = (JSONObject) rootObject.get("nodes");
            Set<?> keys = nodes.keySet();
            Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()) {
            // Iterate on nodes
            // Extract nodesID
            String nodeID = iterator.next().toString();
            // Count nodes
            howManyNodes++;
            // Get Bulk Queue value
            JSONObject node = (JSONObject) nodes.get(nodeID);
            JSONObject threadpool = (JSONObject) node.get("thread_pool");
            JSONObject bulk = (JSONObject) threadpool.get("bulk");
            result += (long) bulk.get("queue");
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result/howManyNodes;
    }

    private static boolean isBulkRejecting(ActionFuture<NodesStatsResponse> nodesStatsResponse){
        //returns true if bulk thread rejections happening;
        boolean result = false;
        JSONParser jsonParser = new JSONParser();
        String rawJSON = nodesStatsResponse.actionGet().toString();
        try {
            JSONObject rootObject = (JSONObject) jsonParser.parse(rawJSON);
            JSONObject nodes = (JSONObject) rootObject.get("nodes");
            Set<?> keys = nodes.keySet();
            Iterator<?> iterator = keys.iterator();
            while (iterator.hasNext()) {
                // Iterate on nodes
                // Extract nodesID
                String nodeID = iterator.next().toString();
                JSONObject node = (JSONObject) nodes.get(nodeID);
                JSONObject threadpool = (JSONObject) node.get("thread_pool");
                JSONObject bulk = (JSONObject) threadpool.get("bulk");
                long rejected = (long) bulk.get("rejected");
                if (rejected > 0) return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static long countIndexDocs(Client client, String idxName) {
        SearchResponse searchResponse = client.prepareSearch().setSearchType(COUNT)
                .setIndices(idxName).execute().actionGet();
        return searchResponse.getHits().getTotalHits();
    }


    private static BulkProcessor.Listener createLoggingBulkProcessorListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                if (DEBUG)
                    System.out.println("DEBUG: Going to execute new bulk composed of {} actions: " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (DEBUG) System.out.println("DEBUG: Executed bulk composed of {} actions: " + request.numberOfActions());
                dstIdxDocCount += request.numberOfActions();
                double percentageDoneLong = (double) dstIdxDocCount / srcIdxDocCount;
                String percentageDoneString = String.valueOf(percentageDoneLong).substring(2, 4);
                if (srcIdxDocCount == dstIdxDocCount) {
                    percentageDoneString = "100";
                }

                System.out.println("INFO: Indexed " + dstIdxDocCount + "/" + srcIdxDocCount + " [ " + percentageDoneString + "% ] from " + srcHost + ":" + srcPort + "/" + srcIndex + " " + " to " + dstHost + ":" + dstPort + "/" + dstIndex + " ");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("ERROR: Error executing bulk: " + failure);
                failure.printStackTrace();
            }
        };
    }
}
