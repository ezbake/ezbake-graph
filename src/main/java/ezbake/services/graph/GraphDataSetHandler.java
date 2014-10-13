/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.services.graph;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.graph.GraphConverter;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.security.client.validation.EzSecurityTokenValidator;
import ezbake.services.graph.archive.EzBroadcastTransactionArchive;
import ezbake.services.graph.archive.WriteTransactionArchive;
import ezbake.services.graph.thrift.EzGraphService;
import ezbake.services.graph.thrift.GraphName;
import ezbake.services.graph.thrift.GraphQuery;
import ezbake.services.graph.thrift.InvalidRequestException;
import ezbake.services.graph.thrift.TransactionId;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.EdgeLabel;
import ezbake.services.graph.thrift.types.ElementId;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.PropValue;
import ezbake.services.graph.thrift.types.PropertyKey;
import ezbake.services.graph.thrift.types.Vertex;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbake.util.AuditLogger;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * This class implements the Thrift GraphService. It uses a GraphStore to delegate all operations to.
 */
public class GraphDataSetHandler extends EzBakeBaseThriftService implements EzGraphService.Iface {

    // Metrics
    private static final String DELETE_VERTICES_TIMER_NAME = MetricRegistry.name(
            GraphDataSetHandler.class, "DELETE", "VERTICES");
    private static final String DELETE_EDGES_TIMER_NAME = MetricRegistry.name(
            GraphDataSetHandler.class, "DELETE", "EDGES");
    private static final String FIND_PATH_TIMER_NAME = MetricRegistry.name(GraphDataSetHandler.class, "FIND", "PATH");
    private static final String SEARCH_FOR_EDGES_TIMER_NAME = MetricRegistry.name(
            GraphDataSetHandler.class, "SEARCH", "FOR", "EDGES");
    private static final String SEARCH_FOR_VERTICES_TIMER_NAME = MetricRegistry.name(
            GraphDataSetHandler.class, "SEARCH", "FOR", "VERTICES");
    private final Logger logger = LoggerFactory.getLogger(GraphDataSetHandler.class);
    private final AuditLogger auditLogger;
    private GraphStore store;
    private String applicationName;
    private String applicationId;
    private WriteTransactionArchive archiver;

    /**
     * Empty constructor does nothing. This constructor is required because ThriftRunner uses it to instantiate an
     * instance via reflection. Then calls EZBakeBaseThriftService.setConfiguration(..). Afterwards, it is expected that
     * the init method be called to make use of the ezbakeconfiguration settings for further dataset runtime
     * configurations.
     */
    public GraphDataSetHandler() throws IOException, InterruptedException {
        auditLogger = AuditLogger.getAuditLogger(GraphDataSetHandler.class);
    }

    protected GraphDataSetHandler(WriteTransactionArchive transactionArchive) {
        archiver = transactionArchive;

        auditLogger = AuditLogger.getAuditLogger(GraphDataSetHandler.class);
    }

    private static void validateGraph(Graph graph) throws InvalidRequestException {
        final List<Vertex> vlist = graph.getVertices();
        for (final Vertex v : vlist) {

            if (v.getId().getSetField() != ElementId._Fields.LOCAL_ID) {
                throw new InvalidRequestException("Error: All vertices in the graph must have a local id.");
            }
        }
    }

    public void init() {
        final EzBakeApplicationConfigurationHelper helper =
                new EzBakeApplicationConfigurationHelper(getConfigurationProperties());
        applicationName = helper.getApplicationName().toLowerCase();
        applicationId = helper.getSecurityID();

        initMetrics();

        if (archiver == null) {
            final String key = "MIIBOwIBAAJBANW5RoOo2nvi53fjOmbL7qkBcr+ejMAaXNJkFkQutpRT65/ytyAQ"
                    + "qakrGSfOYuDjxE70PK5prHV2dNY3jDV+LSMCAwEAAQJATr8ENzB4x9qztF2ZwBR1"
                    + "q/mnoOi3LXTJLI/KEHcxuHaC4mKsbtmEiswPKwNHml4ul3e1FSxOC0aHajvCUIM2"
                    + "oQIhAOyFGiuwn6kn07LPS2Ukb82lZS1Mc/AMGVn6S10vjX/xAiEA51OHBO+rfNFb"
                    + "XX4XDxsDGRGUcJ98/ZW50gGptHxK0lMCIQDGmrGyFBrNWLMMB8MAiAsVvJdr5THJ"
                    + "VO+IvYLBGegQkQIgU/bxj00fRdMIAst9uzHm0fablrWNPM5YAG4yFxz2W5kCIQDZ"
                    + "RsjvMTUHWV2CQgsxxfOwB3C/lhpsJx6xXKej14JE5w==";

            final EzBroadcaster broadcaster = EzBroadcaster.create(
                    getConfigurationProperties(), "graph", key, EzBroadcastTransactionArchive.ARCHIVE_TOPIC_DEFAULT,
                    true);
            try {
                archiver = new EzBroadcastTransactionArchive(broadcaster);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        store = new TitanGraphStore(archiver);
        store.init(getConfigurationProperties());
    }

    @Override
    public TProcessor getThriftProcessor() {
        init();
        return new EzGraphService.Processor(this);
    }

    @Override
    public void shutdown() {
        store.shutdown();
    }

    @Override
    public boolean ping() {
        return store.ping();
    }

    @Override
    public TransactionId createSchema(
            String application, Visibility transactionVisibility, GraphName graphName, List<PropertyKey> keys,
            List<EdgeLabel> labels, EzSecurityToken token) throws InvalidRequestException, TException {
        if (token == null) {
            throw new InvalidRequestException("Error: Missing EzSecurityToken in createSchema call.");
        }

        if (graphName == null) {
            graphName = new GraphName();
        }

        return store.createSchema(application, transactionVisibility, graphName.getName(), keys, labels);
    }

    @Override
    public TransactionId writeGraph(
            String application, Visibility transactionVisibility, GraphName graphName, Graph graph,
            EzSecurityToken token) throws InvalidRequestException, TException {
        if (graphName == null) {
            graphName = new GraphName();
        }

        validateGraph(graph);

        return store.writeGraph(application, transactionVisibility, graphName.getName(), graph);
    }

    @Override
    public TransactionId deleteVertices(
            String application, Visibility transactionVisibility, GraphName graphName, List<Vertex> vertices,
            EzSecurityToken token) throws InvalidRequestException, TException {
        final Timer.Context context =
                getMetricRegistry().getTimers().get(GraphDataSetHandler.DELETE_VERTICES_TIMER_NAME).time();
        try {
            return store.deleteVertices(
                    application, transactionVisibility.getFormalVisibility(), graphName.getName(), vertices,
                    getUserAuths(token));
        } catch (final Exception e) {
            throw new TException(e);
        } finally {
            context.stop();
        }
    }

    @Override
    public TransactionId deleteEdges(
            String application, Visibility transactionVisibility, GraphName graphName, List<Edge> edges,
            EzSecurityToken token) throws InvalidRequestException, TException {

        final Timer.Context context =
                getMetricRegistry().getTimers().get(GraphDataSetHandler.DELETE_EDGES_TIMER_NAME).time();
        try {
            return store.deleteEdges(
                    application, transactionVisibility.getFormalVisibility(), graphName.getName(), edges,
                    getUserAuths(token));
        } catch (final Exception e) {
            throw new TException(e);
        } finally {
            context.stop();
        }
    }

    @Override
    public Vertex getVertex(GraphName graphName, ElementId vertexId, EzSecurityToken token)
            throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "getVertex", description);

        final long titanId = vertexId.getTitanId().getVertexId();

        final com.tinkerpop.blueprints.Vertex vertex =
                store.getVertex(graphName.getName(), titanId, getUserAuths(token));

        if (vertex != null) {
            return GraphConverter.convertElement(vertex);
        } else {
            throw new InvalidRequestException("Invalid vertex id: " + titanId);
        }
    }

    @Override
    public List<Vertex> findVertices(GraphName graphName, String key, PropValue value, EzSecurityToken token)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s   key: %s", graphName.getName(), key);
        auditLog(token, AuditEventType.FileObjectAccess, "findVertices", description);

        final Iterable<com.tinkerpop.blueprints.Vertex> vertices = store.findVertices(
                graphName.getName(), key, GraphConverter.getJavaPropValue(value), getUserAuths(token));
        return GraphConverter.convertVertices(vertices);
    }

    @Override
    public List<Vertex> searchForVertices(GraphName graphName, GraphQuery query, EzSecurityToken token, int limit)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "searchForVertices", description);

        final Timer.Context context =
                getMetricRegistry().getTimers().get(GraphDataSetHandler.SEARCH_FOR_VERTICES_TIMER_NAME).time();
        Iterable<com.tinkerpop.blueprints.Vertex> vertices = null;
        try {
            vertices = store.searchForVertices(graphName.getName(), query, getUserAuths(token), limit);
        } finally {
            context.stop();
        }

        return GraphConverter.convertVertices(vertices);
    }

    @Override
    public Edge getEdge(GraphName graphName, ElementId edgeId, EzSecurityToken token)
            throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "getEdge", description);

        final List<Long> thriftId = edgeId.getTitanId().getEdgeId();
        final long[] titanId = new long[thriftId.size()];
        for (int i = 0; i < titanId.length; i++) {
            titanId[i] = thriftId.get(i);
        }

        final com.tinkerpop.blueprints.Edge edge = store.getEdge(graphName.getName(), titanId, getUserAuths(token));

        if (edge != null) {
            return GraphConverter.convertElement(edge);
        } else {
            throw new InvalidRequestException("Invalid edge id: " + titanId);
        }
    }

    @Override
    public List<Edge> findEdges(GraphName graphName, String key, PropValue value, EzSecurityToken token)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s    key: %s", graphName.getName(), key);
        auditLog(token, AuditEventType.FileObjectAccess, "findEdges", description);

        final Iterable<com.tinkerpop.blueprints.Edge> edges =
                store.findEdges(graphName.getName(), key, GraphConverter.getJavaPropValue(value), getUserAuths(token));
        return GraphConverter.convertEdges(edges);
    }

    @Override
    public List<Edge> searchForEdges(GraphName graphName, GraphQuery query, EzSecurityToken token, int limit)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "searchForEdges", description);

        Iterable<com.tinkerpop.blueprints.Edge> edges = null;
        final Timer.Context context =
                getMetricRegistry().getTimers().get(GraphDataSetHandler.SEARCH_FOR_EDGES_TIMER_NAME).time();

        try {
            edges = store.searchForEdges(graphName.getName(), query, getUserAuths(token), limit);
        } finally {
            context.stop();
        }

        return GraphConverter.convertEdges(edges);
    }

    @Override
    public Graph expandSubgraph(GraphName graphName, Vertex rootVertex, int numberOfHops, EzSecurityToken token)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectModify, "expandSubgraph", description);

        try {
            return store.expandSubgraph(graphName.getName(), rootVertex, numberOfHops, getUserAuths(token));
        } catch (final InvalidHopSizeException ihse) {
            throw new TException(ihse);
        }
    }

    @Override
    public Graph findPath(
            GraphName graphName, Vertex startVertex, Vertex endVertex, int maxHops, EzSecurityToken token)
            throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "findPath", description);

        final Timer.Context context =
                getMetricRegistry().getTimers().get(GraphDataSetHandler.FIND_PATH_TIMER_NAME).time();
        try {
            return store.findPath(graphName.getName(), startVertex, endVertex, maxHops, getUserAuths(token));
        } catch (final InvalidHopSizeException ihse) {
            throw new TException(ihse);
        } finally {
            context.stop();
        }
    }

    @Override
    public List<Vertex> queryVertices(
            GraphName graphName, Vertex startVertex, String gremlinQuery, EzSecurityToken token)
            throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("graph name: %s ", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "queryVertices", description);

        final Iterable<com.tinkerpop.blueprints.Vertex> vertices =
                store.queryVertices(graphName.getName(), startVertex, gremlinQuery, getUserAuths(token));
        return GraphConverter.convertVertices(vertices);
    }

    @Override
    public List<Edge> queryEdges(GraphName graphName, Vertex startVertex, String gremlinQuery, EzSecurityToken token)
            throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "queryEdges", description);

        final Iterable<com.tinkerpop.blueprints.Edge> edges =
                store.queryEdges(graphName.getName(), startVertex, gremlinQuery, getUserAuths(token));
        return GraphConverter.convertEdges(edges);
    }

    public TransactionId deleteVertices(
            String application, String transactionVisibility, GraphName graphName, List<Vertex> vertices,
            EzSecurityToken token) throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("application: %s    graph name: %s", application, graphName.getName());
        auditLog(token, AuditEventType.FileObjectDelete, "deleteVertices", description);

        return store.deleteVertices(
                application, transactionVisibility, graphName.getName(), vertices, getUserAuths(token));
    }

    public TransactionId deleteEdges(
            String application, String transactionVisibility, GraphName graphName, List<Edge> edges,
            EzSecurityToken token) throws InvalidRequestException, TException {
        validateToken(token);

        final String description = String.format("application: %s  graph name: %s", application, graphName.getName());
        auditLog(token, AuditEventType.FileObjectDelete, "deleteEdges", description);

        return store.deleteEdges(application, transactionVisibility, graphName.getName(), edges, getUserAuths(token));
    }

    public List<Vertex> getVertices(GraphName graphName, EzSecurityToken token) throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "getVertices", description);

        final Iterable<com.tinkerpop.blueprints.Vertex> vertices =
                store.getVertices(graphName.getName(), getUserAuths(token));
        return GraphConverter.convertVertices(vertices);
    }

    public List<Edge> getEdges(GraphName graphName, EzSecurityToken token) throws TException {
        validateToken(token);

        final String description = String.format("graph name: %s", graphName.getName());
        auditLog(token, AuditEventType.FileObjectAccess, "getEdges", description);
        final Iterable<com.tinkerpop.blueprints.Edge> edges = store.getEdges(graphName.getName(), getUserAuths(token));
        return GraphConverter.convertEdges(edges);
    }

    private void auditLog(EzSecurityToken userToken, AuditEventType eventType, String action, String description) {
        final AuditEvent auditEvent = new AuditEvent(eventType, userToken).arg(
                "security app",
                userToken.isSetValidity() ? userToken.getValidity().getIssuedTo() : "N/A - No Application supplied")
                .arg(
                        "user", userToken.isSetTokenPrincipal() ? userToken.getTokenPrincipal().getPrincipal() :
                                "N/A - Service Request").arg("action", action).arg("application", applicationName)
                .arg("description", description);

        auditLogger.logEvent(auditEvent);
    }

    private void initMetrics() {
        final MetricRegistry mr = getMetricRegistry();

        mr.timer(GraphDataSetHandler.DELETE_EDGES_TIMER_NAME);
        mr.timer(GraphDataSetHandler.DELETE_VERTICES_TIMER_NAME);
        mr.timer(GraphDataSetHandler.FIND_PATH_TIMER_NAME);
        mr.timer(GraphDataSetHandler.SEARCH_FOR_EDGES_TIMER_NAME);
        mr.timer(GraphDataSetHandler.SEARCH_FOR_VERTICES_TIMER_NAME);
    }

    private void validateToken(EzSecurityToken token) throws TException {
        EzSecurityTokenValidator.validateToken(token, getConfigurationProperties());
        String fromId = token.getValidity().getIssuedTo();
        if (!fromId.equals(applicationId)) {
            throw new TException("Mismatched Security Id's: " + fromId + " != " + applicationId);
        }
    }

    private String getUserAuths(EzSecurityToken token) {
        Set<String> a = token.getAuthorizations().getFormalAuthorizations();
        return (a == null) ? "" : Joiner.on(',').join(a);
    }
}
