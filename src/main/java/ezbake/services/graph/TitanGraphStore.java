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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.LabelMaker;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloSecurityToken;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.secure.SecureGraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanGraph;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanTx;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.gremlin.groovy.GremlinGroovyPipeline;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;

import ezbake.base.thrift.Visibility;
import ezbake.data.common.graph.GraphConverter;
import ezbake.data.common.graph.TitanGraphConfiguration;
import ezbake.services.graph.archive.TransactionArchiveException;
import ezbake.services.graph.archive.TransactionIdGenerator;
import ezbake.services.graph.archive.WriteTransactionArchive;
import ezbake.services.graph.thrift.EzGraphServiceConstants;
import ezbake.services.graph.thrift.GraphQuery;
import ezbake.services.graph.thrift.GraphQueryTerm;
import ezbake.services.graph.thrift.InvalidQueryException;
import ezbake.services.graph.thrift.InvalidRequestException;
import ezbake.services.graph.thrift.QueryPredicate;
import ezbake.services.graph.thrift.SortDirection;
import ezbake.services.graph.thrift.Transaction;
import ezbake.services.graph.thrift.TransactionId;
import ezbake.services.graph.thrift.types.EdgeLabel;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.GraphTypesConstants;
import ezbake.services.graph.thrift.types.Index;
import ezbake.services.graph.thrift.types.IndexName;
import ezbake.services.graph.thrift.types.Property;
import ezbake.services.graph.thrift.types.PropertyKey;

/**
 * This class provides Titan specific implementation for the GraphStore interface. This class is not depending on a
 * thrift service and is used as the delegation class by GraphDataSetHandler.
 */
public class TitanGraphStore implements GraphStore {

    private static final String SEL_KEY_SEP = "/";

    private static final Set<String> validPipes = Sets.newHashSet(
            "GremlinStartPipe", "AndFilterPipe", "LabelFilterPipe", "GatherPipe", "GraphQueryPipe", "IdVertexPipe",
            "FutureFilterPipe", "EdgesVerticesPipe", "CollectionFilterPipe", "InPipe", "InEdgesPipe", "ScatterPipe",
            "InVertexPipe", "PropertyFilterPipe", "PathPipe", "BothEdgesPipe", "OutPipe", "QueryPipe",
            "PropertyMapPipe", "RetainFilterPipe", "HasCountPipe", "CyclicPathFilterPipe", "TransformPipe",
            "SideEffectCapPipe", "LabelPipe", "TransformFunctionPipe", "OrFilterPipe", "VerticesEdgesPipe",
            "BackFilterPipe", "ExceptFilterPipe", "IdEdgePipe", "IdPipe", "ObjectFilterPipe", "IdFilterPipe",
            "VertexQueryPipe", "BothVerticesPipe", "SelectPipe", "GatherFunctionPipe", "IndexElementsPipe",
            "FilterFunctionPipe", "ShufflePipe", "OutEdgesPipe", "HasNextPipe", "ToStringPipe", "FilterPipe",
            "BothPipe", "OutVertexPipe", "RandomFilterPipe", "PropertyPipe", "OrderMapPipe", "VerticesVerticesPipe",
            "DuplicateFilterPipe", "MemoizePipe", "RangeFilterPipe", "OrderPipe", "IntervalFilterPipe");
    private final Logger logger = LoggerFactory.getLogger(TitanGraphStore.class);
    private final LoadingCache<String, SecureTitanGraph<AccumuloSecurityToken>> graphCache;
    /**
     * Stores selector vertices for all subgraphs
     */
    private final Map<String, Cache<String, Long>> graphSelectorCache;
    private final WriteTransactionArchive transactionArchive;
    private final TransactionIdGenerator transIdGenerator;
    private TitanGraphConfiguration config;

    /**
     * Constructor creates graph cache
     *
     * @param archive
     */
    public TitanGraphStore(WriteTransactionArchive archive) {
        transactionArchive = archive;
        transIdGenerator = new TransactionIdGenerator();

        final RemovalListener<String, SecureTitanGraph<AccumuloSecurityToken>> graphRemover =
                new RemovalListener<String, SecureTitanGraph<AccumuloSecurityToken>>() {
                    @Override
                    public void onRemoval(
                            RemovalNotification<String, SecureTitanGraph<AccumuloSecurityToken>> removal) {
                        final SecureTitanGraph<AccumuloSecurityToken> g = removal.getValue();
                        if (g != null) {
                            g.shutdown();
                        }
                    }
                };

        final CacheLoader<String, SecureTitanGraph<AccumuloSecurityToken>> graphLoader =
                new CacheLoader<String, SecureTitanGraph<AccumuloSecurityToken>>() {
                    @Override
                    public SecureTitanGraph<AccumuloSecurityToken> load(String graphName) {
                        final Cache<String, Long> selectorCache = CacheBuilder.newBuilder().maximumSize(1000).build();
                        graphSelectorCache.put(graphName, selectorCache);
                        return newInstanceGraph(graphName);
                    }
                };

        // create graph cache for graph instances
        graphCache = CacheBuilder.newBuilder().maximumSize(100).removalListener(graphRemover).build(graphLoader);

        graphSelectorCache = Maps.newHashMap();
    }

    private static Predicate convertThriftQueryPredicate(QueryPredicate predicate) {
        switch (predicate) {
            case EQUAL:
                return Compare.EQUAL;
            case NOT_EQUAL:
                return Compare.NOT_EQUAL;
            case LESS_THAN:
                return Compare.LESS_THAN;
            case LESS_THAN_EQUAL:
                return Compare.LESS_THAN_EQUAL;
            case GREATER_THAN:
                return Compare.GREATER_THAN;
            case GREATER_THAN_EQUAL:
                return Compare.GREATER_THAN_EQUAL;
            case CONTAINS:
                return Text.CONTAINS;
            case CONTAINS_PREFIX:
                return Text.CONTAINS_PREFIX;
            case CONTAINS_REGEX:
                return Text.CONTAINS_REGEX;
            case PREFIX:
                return Text.PREFIX;
            case REGEX:
                return Text.REGEX;
            case INTERSECT:
                return Geo.INTERSECT;
            case WITHIN:
                return Geo.WITHIN;
        }
        return null;
    }

    private static Order convertThriftSortDirection(SortDirection direction) {
        if (direction == SortDirection.ASCENDING) {
            return Order.ASC;
        } else {
            return Order.DESC;
        }
    }

    @Override
    public void init(Properties props) {
        this.config = new TitanGraphConfiguration(props);

        // initialize the transaction archive
        if (transactionArchive != null) {
            try {
                transactionArchive.init(props);
            } catch (final TransactionArchiveException tae) {
                log("Unable to initialize Transaction Archive", tae);
            }
        }
    }

    @Override
    public boolean ping() {
        return true;
    }

    @Override
    public TransactionId createSchema(
            String application, Visibility transactionVisibility, String graphName, List<PropertyKey> keys,
            List<EdgeLabel> labels) throws InvalidRequestException {

        validateApplication(application);

        validateVisibility(getVisibility(transactionVisibility));

        final TransactionId tid = getTransactionId();

        final TitanGraph graph = openGraph(graphName);

        // ensure unique key is created for selector vertices
        if (graph.getType(EzGraphServiceConstants.UNIQUE_KEY) == null) {
            graph.makeKey(EzGraphServiceConstants.UNIQUE_KEY).dataType(String.class).indexed(Vertex.class).unique()
                    .make();
        }

        makePropertyKeys(graph, keys);
        makeEdgeLabels(graph, labels);

        try {
            graph.commit();
        } catch (final TitanException ex) {
            final InvalidRequestException ire = new InvalidRequestException("Error making types: " + ex.getMessage());
            log(ire.getMessage(), ex);
            throw ire;
        }

        // if the commit succeeded, then write to the archive
        archiveTypes(tid, application, transactionVisibility, keys, labels);
        return tid;
    }

    @Override
    public TransactionId writeGraph(
            String application, Visibility transactionVisibility, String graphName, Graph thriftGraph)
            throws InvalidRequestException {
        // perform parameter validation checks
        validateApplication(application);

        validateVisibility(getVisibility(transactionVisibility));

        if (thriftGraph == null) {
            throw new InvalidRequestException("Subgraph cannot be null");
        }

        final TransactionId tid = getTransactionId();

        // write out subgraph
        // create or get subgraph reference from cache
        final SecureTitanGraph<AccumuloSecurityToken> titanGraph = openGraph(graphName);

        // validate that a schema has been created for this graph
        if (titanGraph.getType(EzGraphServiceConstants.UNIQUE_KEY) == null) {
            final InvalidRequestException ire = new InvalidRequestException(
                    "User attempted to write a graph before creating a schema: " + graphName);
            log(ire.getMessage(), ire);
            throw ire;
        }

        // store the subgraph's vertex ids to Titan generated Ids for later use
        // with edge connecting
        final Map<String, Object> vertexIdMapping = Maps.newHashMap();

        // if we have vertices, convert each vertex and its properties to
        // TitanVertex objects,
        // if we have a selector property on the vertex, then we'll need to add
        // the vertex special case
        // if we don't have any vertices, then we also don't have any edges
        if (thriftGraph.isSetVertices()) {
            processVertices(graphName, thriftGraph, titanGraph, vertexIdMapping);

            if (thriftGraph.isSetEdges()) {
                processEdges(graphName, thriftGraph, vertexIdMapping);
            }
        }

        // write out to archive
        archiveGraph(tid, application, transactionVisibility, thriftGraph);
        return tid;
    }

    @Override
    public TransactionId deleteVertices(
            String application, String transactionVisibility, String graphName,
            List<ezbake.services.graph.thrift.types.Vertex> vertices, String authorizations)
            throws InvalidRequestException {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
        // Tools | Templates.
    }

    @Override
    public TransactionId deleteEdges(
            String application, String transactionVisibility, String graphName,
            List<ezbake.services.graph.thrift.types.Edge> edges, String authorizations) throws InvalidRequestException {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
        // Tools | Templates.
    }

    @Override
    public Vertex getVertex(String graphName, Object id, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Vertex vertex = tx.getVertex(id);
        return vertex;
    }

    @Override
    public Iterable<Vertex> getVertices(String graphName, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Iterable<Vertex> vertices = tx.getVertices();
        return vertices;
    }

    @Override
    public Iterable<Vertex> findVertices(
            String graphName, String key, Object value, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Iterable<Vertex> vertices = tx.getVertices(key, value);
        return vertices;
    }

    @Override
    public Iterable<Vertex> searchForVertices(
            String graphName, GraphQuery query, String authorizations, int limit) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final com.tinkerpop.blueprints.GraphQuery blueQuery = makeQuery(tx, query);
        limitQuery(blueQuery, limit);
        return blueQuery.vertices();
    }

    @Override
    public Edge getEdge(String graphName, Object edgeId, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Edge edge = tx.getEdge(edgeId);
        return edge;
    }

    @Override
    public Iterable<Edge> getEdges(String graphName, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Iterable<Edge> edges = tx.getEdges();
        return edges;
    }

    @Override
    public Iterable<Edge> findEdges(
            String graphName, String key, Object value, String authorizations) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Iterable<Edge> edges = tx.getEdges(key, value);
        return edges;
    }

    @Override
    public Iterable<Edge> searchForEdges(
            String graphName, GraphQuery query, String authorizations, int limit) {
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final com.tinkerpop.blueprints.GraphQuery blueQuery = makeQuery(tx, query);
        limitQuery(blueQuery, limit);
        return blueQuery.edges();
    }

    @Override
    public Graph expandSubgraph(
            String graphName, ezbake.services.graph.thrift.types.Vertex root, int maxHops, String authorizations)
            throws InvalidHopSizeException {
        final int MAXIMUM_EXPAND_HOP_LIMIT = 8;
        if (maxHops < 0 || maxHops > MAXIMUM_EXPAND_HOP_LIMIT) {
            throw new InvalidHopSizeException(maxHops);
        }

        // make sure to first get the startVertex under the right authority
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Vertex startVertex = tx.getVertex(root.getId().getTitanId().getVertexId());

        return EzBreadthFirstSearch.bfs(
                startVertex, maxHops, new Function<com.tinkerpop.blueprints.Vertex, Boolean>() {
                    @Override
                    public Boolean apply(com.tinkerpop.blueprints.Vertex vertex) {
                        return true;
                    }
                });
    }

    @Override
    public Graph findPath(
            String graphName, ezbake.services.graph.thrift.types.Vertex startVertex,
            final ezbake.services.graph.thrift.types.Vertex endVertex, int maxHops, String authorizations)
            throws InvalidHopSizeException {
        final int MAXIMUM_PATH_HOP_LIMIT = 8;
        if (maxHops < 1 || maxHops > MAXIMUM_PATH_HOP_LIMIT) {
            throw new InvalidHopSizeException(maxHops);
        }

        // make sure to first get the startVertex under the right authority
        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);
        final Vertex startV = tx.getVertex(startVertex.getId().getTitanId().getVertexId());

        // perform BFS to find the matching end vertex, we use BFS first because it can be faster

        final Graph bfsSubgraph = EzBreadthFirstSearch.bfs(
                startV, maxHops, new Function<com.tinkerpop.blueprints.Vertex, Boolean>() {
                    @Override
                    public Boolean apply(com.tinkerpop.blueprints.Vertex vertex) {
                        final boolean result = vertex.getId().equals(endVertex.getId().getTitanId().getVertexId());
                        return !result;
                    }
                });

        return bfsSubgraph;
    }

    @Override
    public Iterable<Vertex> queryVertices(
            String graphName, ezbake.services.graph.thrift.types.Vertex startVertex, String gremlinQuery,
            String authorizations) throws InvalidQueryException {

        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);

        final GremlinGroovyPipeline pipeline = queryHelper(gremlinQuery);

        pipeline.setStarts(new SingleIterator<Vertex>(tx.getVertex(startVertex.getId().getTitanId().getVertexId())));
        final Iterator<Vertex> vertexIterator = pipeline.iterator();

        return new Iterable<Vertex>() {
            @Override
            public Iterator<Vertex> iterator() {
                return vertexIterator;
            }
        };
    }

    @Override
    public Iterable<Edge> queryEdges(
            String graphName, ezbake.services.graph.thrift.types.Vertex startVertex, String gremlinQuery,
            String authorizations) throws InvalidQueryException {

        final SecureTitanTx<AccumuloSecurityToken> tx = startReadTransaction(graphName, authorizations);

        final GremlinGroovyPipeline pipeline = queryHelper(gremlinQuery);

        pipeline.setStarts(new SingleIterator<Vertex>(tx.getVertex(startVertex.getId().getTitanId().getVertexId())));
        final Iterator<Edge> edgeIterator = pipeline.iterator();

        return new Iterable<Edge>() {
            @Override
            public Iterator<Edge> iterator() {
                return edgeIterator;
            }
        };
    }

    @Override
    public void shutdown() {
        // shuts down all titan graph references
        graphCache.invalidateAll();
    }

    protected final void log(String msg, Exception e) {
        if (!StringUtils.isEmpty(msg)) {
            logger.error(msg);
        }

        if (e != null) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
    }

    private GremlinGroovyPipeline queryHelper(String gremlinQuery) throws InvalidQueryException {
        final GremlinGroovyPipeline pipeline = (GremlinGroovyPipeline) Gremlin.compile(gremlinQuery);
        final List<Pipe> pipes = pipeline.getPipes();

        for (final Pipe pipe : pipes) {
            if (!validPipes.contains(pipe.getClass().getSimpleName())) {
                throw new InvalidQueryException(String.format("Contains class %s", pipe.getClass().getName()));
            }
        }

        return pipeline;
    }

    private void limitQuery(Query query, int limit) {
        final int MAX_QUERY_RESULT_SIZE = 2 << 13;
        if (limit > MAX_QUERY_RESULT_SIZE) {
            query.limit(MAX_QUERY_RESULT_SIZE);
        } else if (limit == 0) {
            final int DEFAULT_QUERY_RESULT_SIZE = 2 << 12;
            query.limit(DEFAULT_QUERY_RESULT_SIZE);
        } else {
            query.limit(limit);
        }
    }

    private SecureTitanTx startReadTransaction(String graphName, String authorizations) {
        return openGraph(graphName).buildTransaction()
                .setReadToken(AccumuloSecurityToken.getInstance().newReadToken(authorizations)).start();
    }

    private SecureTitanTx startWriteTransaction(String graphName, String visibility) {
        return openGraph(graphName).buildTransaction()
                .setWriteToken(AccumuloSecurityToken.getInstance().newWriteToken(visibility)).start();
    }

    /**
     * Creates a new graph instance for the specified graph name. This method is used by LoadingCache.
     */
    private SecureTitanGraph<AccumuloSecurityToken> newInstanceGraph(String graphName) {
        String graphName1 = graphName;
        if (StringUtils.isEmpty(graphName1)) {
            graphName1 = EzGraphServiceConstants.GLOBAL_GRAPH;
        }

        // first we clone the current configuration, this gives us a deep copy
        final Configuration conf = (Configuration) config.clone();

        // now we override the graph name from the original config with the new
        // specified one
        final Configuration storageConf = conf.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storageConf.setProperty(AccumuloStoreManager.TABLE_NAME_KEY, graphName1);

        // finally, create new graph instance and return it
        final SecureTitanGraph<AccumuloSecurityToken> stg =
                new SecureTitanGraph<>(new SecureGraphDatabaseConfiguration(conf));

        return stg;
    }

    /*
     * private void printVertex(com.tinkerpop.blueprints.Vertex v) { System.err.println("Vertex ID: " + v.getId());
     * for (String key : v.getPropertyKeys()) { System.err.println("\t" + key + " : " + v.getProperty(key)); } }
     */

    private SecureTitanGraph<AccumuloSecurityToken> openGraph(String graphName) {
        Preconditions.checkArgument(graphName != null);
        try {
            return graphCache.get(graphName);
        } catch (final ExecutionException ee) {
            logger.error("Error retrieving graph from cache: {}", graphName);
            logger.error(ExceptionUtils.getStackTrace(ee));
            return null;
        }
    }

    /**
     * Adds all vertices and their properties from the specified Thrift graph to the Titan graph. All properties of
     * vertices are grouped by visibility to minimize the number of created transactions. The total number of
     * transactions should be approximately the same number of visibility variants.
     */
    private void processVertices(
            String graphName, Graph thriftGraph, final SecureTitanGraph<AccumuloSecurityToken> titanGraph,
            Map<String, Object> vertexIdMapping) throws InvalidRequestException {
        final List<ezbake.services.graph.thrift.types.Vertex> thriftVertices = thriftGraph.getVertices();

        // stores first by visibility string, then by titan vertex id
        // the second paramter needs to be a map so that we don't end up creating
        // duplicate VertexRecords with the same titan id since properties aren't sorted by visibility
        final Map<String, Map<Object, VertexRecord>> vertexRecords = Maps.newHashMap();

        // first loop creates a vertex and adds the titanid to localid mapping, and
        // organizes verticies->properties by visibility
        for (final ezbake.services.graph.thrift.types.Vertex thriftVertex : thriftVertices) {
            Object titanVertexId = null;
            final Map<String, List<Property>> props = thriftVertex.getProperties();

            // selector vertices are created using a different method
            if (thriftVertex.isSetSelectorProperty()) {
                final String selectorKey = thriftVertex.getSelectorProperty();
                final Property selProp = props.get(selectorKey).get(0);

                // create the unique selector key for the cache
                final String selectorVal = GraphConverter.getJavaPropValue(selProp.getValue()).toString();
                try {
                    final Cache<String, Long> selectorCache = graphSelectorCache.get(graphName);
                    titanVertexId = selectorCache.get(
                            selectorKey + SEL_KEY_SEP + selectorVal, new Callable<Long>() {
                                @Override
                                public Long call() throws Exception {
                                    final SelectorVertexFactory vertexFactory = new SelectorVertexFactory(titanGraph);
                                    final Long id = vertexFactory.getSelectorId(selectorKey, selectorVal);
                                    return id;
                                }
                            });
                } catch (final ExecutionException e) {
                    log("Error loading selector vertex into cache.", e);
                    throw new InvalidRequestException("Error loading selector vertex into cache: " + e.getMessage());
                }
            } else {
                final SecureTitanTx<AccumuloSecurityToken> tx = titanGraph.newTransaction();
                final TitanVertex titanVertex = tx.addVertex(null);
                tx.commit();
                titanVertexId = titanVertex.getId();
            }

            // add the vertex id mapping so edges can be properly added later
            // no need to rollback these entries since it's scope is not persistent
            // and is only used if adding all vertices was successful
            vertexIdMapping.put(thriftVertex.getId().getLocalId(), titanVertexId);

            // capture and categorize by visibility the properties of this vertex
            if (props != null) {
                for (final Map.Entry<String, List<Property>> entry : props.entrySet()) {
                    final String name = entry.getKey();
                    for (final Property prop : entry.getValue()) {
                        // first, get the visibililty of each property
                        final String visi = getVisibility(prop.getVisibility());

                        // second, get the map of vertex id to record by visibility
                        Map<Object, VertexRecord> rec = vertexRecords.get(visi);
                        if (rec == null) {
                            rec = Maps.newHashMap();
                        }

                        // third, get the actual vertex record by vertex id
                        // then add the property and update maps
                        VertexRecord vr = rec.get(titanVertexId);
                        if (vr == null) {
                            vr = new VertexRecord(visi);
                        }
                        vr.addProperty(name, prop);

                        rec.put(titanVertexId, vr);
                        vertexRecords.put(visi, rec);
                    }
                }
            }
        }

        // now create the transactions to save off the properties at the respective visibilities,
        // the number of transactions will equal the number of variant visibilities for all vertex properties
        // in the thrift graph
        for (final Map.Entry<String, Map<Object, VertexRecord>> stringMapEntry : vertexRecords.entrySet()) {
            final SecureTitanTx<AccumuloSecurityToken> tx = startWriteTransaction(graphName, stringMapEntry.getKey());
            final Map<Object, VertexRecord> recs = stringMapEntry.getValue();
            for (final Map.Entry<Object, VertexRecord> objectVertexRecordEntry : recs.entrySet()) {
                final VertexRecord rec = objectVertexRecordEntry.getValue();
                final TitanVertex tv = tx.getVertex(objectVertexRecordEntry.getKey());
                for (final Map.Entry<String, List<Property>> entry : rec.getProperties().entrySet()) {
                    final String name = entry.getKey();
                    for (final Property prop : entry.getValue()) {
                        tv.addProperty(name, GraphConverter.getJavaPropValue(prop.getValue()));
                    }
                }
            }
            tx.commit();
        }
    }

    private void processEdges(
            String graphName, Graph thriftGraph, Map<String, Object> vertexIdMapping) {
        final Map<String, List<ezbake.services.graph.thrift.types.Edge>> edges =
                groupEdgesByVisi(thriftGraph.getEdges());

        // add edges with respective visibility transactions
        for (final Map.Entry<String, List<ezbake.services.graph.thrift.types.Edge>> stringListEntry : edges
                .entrySet()) {
            final SecureTitanTx<AccumuloSecurityToken> tx = startWriteTransaction(graphName, stringListEntry.getKey());
            for (final ezbake.services.graph.thrift.types.Edge thriftEdge : stringListEntry.getValue()) {
                final TitanVertex outVertex = tx.getVertex(vertexIdMapping.get(thriftEdge.getOutVertex().getLocalId()));
                final TitanVertex inVertex = tx.getVertex(vertexIdMapping.get(thriftEdge.getInVertex().getLocalId()));

                final TitanEdge titanEdge = tx.addEdge(outVertex, inVertex, thriftEdge.getLabel());

                final Map<String, Property> props = thriftEdge.getProperties();

                for (final Map.Entry<String, Property> stringPropertyEntry : props.entrySet()) {
                    titanEdge.setProperty(
                            stringPropertyEntry.getKey(), GraphConverter.getJavaPropValue(
                                    stringPropertyEntry.getValue().getValue()));
                }
            }
            tx.commit();
        }
    }

    /**
     * Returns a map where key is visibility string and value is list of edges that are of that visibility type.
     *
     * @param edges
     * @return
     */
    private Map<String, List<ezbake.services.graph.thrift.types.Edge>> groupEdgesByVisi(
            List<ezbake.services.graph.thrift.types.Edge> edges) {
        final Map<String, List<ezbake.services.graph.thrift.types.Edge>> visiProps = Maps.newHashMap();

        for (final ezbake.services.graph.thrift.types.Edge edge : edges) {
            final String visi = getVisibility(edge.getVisibility());
            List<ezbake.services.graph.thrift.types.Edge> list = visiProps.get(visi);
            if (list == null) {
                list = Lists.newArrayList();
            }
            list.add(edge);
            visiProps.put(visi, list);
        }
        return visiProps;
    }

    private void validateApplication(String application) throws InvalidRequestException {
        if (StringUtils.isEmpty(application)) {
            throw new InvalidRequestException("Invalid application: cannot be empty or null");
        }
    }

    private void validateVisibility(String visibility) throws InvalidRequestException {
        Preconditions.checkArgument(visibility != null);
        try {

            final ColumnVisibility vis = new ColumnVisibility(visibility);
        } catch (final BadArgumentException ex) {
            log(ex.getMessage(), ex);
            throw new InvalidRequestException("Invalid visibility: " + visibility);
        }
    }

    private String getVisibility(Visibility cla) {
        return cla.getFormalVisibility();
    }

    private TransactionId getTransactionId() throws InvalidRequestException {
        try {
            return transIdGenerator.nextId();
        } catch (final IllegalStateException ex) {
            throw new InvalidRequestException("Transaction ID generator failed: " + ex.getMessage());
        }
    }

    private void archiveTypes(
            TransactionId tid, String application, Visibility visibility, List<PropertyKey> keys,
            List<EdgeLabel> labels) throws InvalidRequestException {
        try {
            final Transaction transaction = new Transaction(tid, application, visibility);

            if (keys != null) {
                transaction.setPropertyKeys(keys);
            }

            if (labels != null) {
                transaction.setEdgeLabels(labels);
            }

            transactionArchive.write(transaction);
        } catch (final TransactionArchiveException ex) {
            log("Error writing types to archive", ex);
            throw new InvalidRequestException("Error writing types to acrhive: " + ex.getMessage()).setTid(tid);
        }
    }

    private void archiveGraph(
            TransactionId tid, String application, Visibility visibility, Graph thriftGraph)
            throws InvalidRequestException {
        try {
            final Transaction transaction = new Transaction(tid, application, visibility);
            transaction.setGraph(thriftGraph);

            transactionArchive.write(transaction);
        } catch (final TransactionArchiveException ex) {
            log("Error writing graph to archive", ex);
            throw new InvalidRequestException("Error writing types to acrhive: " + ex.getMessage()).setTid(tid);
        }
    }

    private void makePropertyKeys(TitanGraph graph, List<PropertyKey> keys) throws InvalidRequestException {
        if (keys != null && !keys.isEmpty()) {
            for (final PropertyKey key : keys) {
                try {
                    if (graph.getType(key.getName()) == null) {
                        final KeyMaker keyMaker = graph.makeKey(key.getName());

                        // keyMaker.list();
                        setPropertyDataType(keyMaker, key);
                        setPropertyIndices(keyMaker, key);

                        keyMaker.make();
                    }
                } catch (final IllegalArgumentException ex) {
                    throw new InvalidRequestException("Error making property key " + key + ": " + ex.getMessage());
                }
            }
        }
    }

    private void makeEdgeLabels(TitanGraph graph, List<EdgeLabel> labels) throws InvalidRequestException {
        if (labels != null) {
            for (final EdgeLabel label : labels) {
                try {
                    final LabelMaker labelMaker = graph.makeLabel(label.getName());
                    setEdgeSortKey(graph, labelMaker, label);
                    labelMaker.make();
                } catch (final IllegalArgumentException ex) {
                    throw new InvalidRequestException("Error making edge label " + label + ": " + ex.getMessage());
                }
            }
        }
    }

    private void setPropertyDataType(KeyMaker keyMaker, PropertyKey key) {
        if (key.isSetDataType()) {
            keyMaker.dataType(GraphConverter.getDataTypeClass(key.getDataType()));
        } else {
            keyMaker.dataType(Object.class);
        }
    }

    private void setPropertyIndices(KeyMaker keyMaker, PropertyKey key) {
        if (key.isSetIndices()) {
            for (final Index index : key.getIndices()) {
                // create a standard index
                final Class elmClass = GraphConverter.getElementClass(index.getElement());
                keyMaker.indexed(elmClass);

                if (index.isSetIndexName() && IndexName.SEARCH == index.getIndexName()) {
                    keyMaker.indexed(GraphTypesConstants.SEARCH_INDEX, elmClass);
                } else {
                    keyMaker.indexed(elmClass);
                }
            }
        }
    }

    private void setEdgeSortKey(
            TitanGraph graph, LabelMaker labelMaker, EdgeLabel label) {
        if (label.isSetSortKey()) {
            final List<TitanKey> sortKeys = Lists.newArrayList();
            for (final String key : label.getSortKey()) {
                sortKeys.add((TitanKey) graph.getType(key));
            }

            labelMaker.sortKey(sortKeys.toArray(new TitanKey[sortKeys.size()]));
        }
    }

    private com.tinkerpop.blueprints.GraphQuery makeQuery(TitanTransaction tx, GraphQuery query) {
        TitanGraphQuery graphQuery = tx.query();
        boolean hasAddedTerms = false;
        for (final GraphQueryTerm term : query.getTerms()) {
            final Predicate blueprintsPredicate = convertThriftQueryPredicate(term.getPredicate());
            if (blueprintsPredicate != null) {
                graphQuery = graphQuery.has(term.getFieldName(), blueprintsPredicate, term.getValue());
                hasAddedTerms = true;
            }
        }
        if (hasAddedTerms) {
            if (query.isSetSortOptions()) {
                graphQuery = graphQuery.orderBy(
                        query.getSortOptions().getSortField(),
                        convertThriftSortDirection(query.getSortOptions().getDirection()));
            }
        }
        return graphQuery;
    }

    // used to store a vertex and its properties that are of all
    // the same visibility
    static class VertexRecord {

        private final Map<String, List<Property>> properties;

        public VertexRecord(String visi) {
            properties = Maps.newHashMap();
        }

        public void addProperty(String name, Property prop) {
            if (!properties.containsKey(name)) {
                properties.put(name, Lists.<Property>newArrayList());
            }
            properties.get(name).add(prop);
        }

        public Map<String, List<Property>> getProperties() {
            return properties;
        }
    }
}
