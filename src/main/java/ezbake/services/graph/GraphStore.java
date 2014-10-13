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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.services.graph.thrift.*;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.Vertex;
import ezbake.services.graph.thrift.types.EdgeLabel;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.PropertyKey;

import java.util.List;
import java.util.Properties;

public interface GraphStore {

    /**
     * Perform any kind of initialization on graph store.
     *
     * @param extraConfig Configuration options for store.
     */
    public void init(Properties extraConfig);

    /**
     * Ping graph store.
     *
     * @return True if store is up, false otherwise
     */
    public boolean ping();

    /**
     * Create schema that defines the property keys and edge labels for graph.
     *
     * @param application Name of application defining schema
     * @param transactionVisibility Visibility of schema transaction
     * @param graphName Name of graph
     * @param keys List of property key definitions
     * @param labels List of edge label definitions
     * @return Transaction id
     * @throws InvalidRequestException If schema definition has error
     */
    public TransactionId createSchema(
            String application, Visibility transactionVisibility, String graphName, List<PropertyKey> keys,
            List<EdgeLabel> labels) throws InvalidRequestException;

    /**
     * Write subgraph to the named graph.
     *
     * @param application Name of application defining schema
     * @param transactionVisibility Visibility of write transaction
     * @param graphName Name of graph
     * @param graph Subgraph to write
     * @return Transaction id
     * @throws ezbake.services.graph.thrift.InvalidRequestException If schema definition has error
     */
    public TransactionId writeGraph(String application, Visibility transactionVisibility, String graphName, Graph graph)
            throws InvalidRequestException;

    /**
     * Delete vertices from named graph.
     *
     * @param application Name of application defining schema
     * @param transactionVisibility Visibility of delete transaction
     * @param graphName Name of graph
     * @param vertices List of vertices to delete
     * @param authorizations Authorizations for transaction
     * @return Transaction id
     * @throws ezbake.services.graph.thrift.InvalidRequestException If schema definition has error
     */
    public TransactionId deleteVertices(
            String application, String transactionVisibility, String graphName, List<Vertex> vertices,
            String authorizations) throws InvalidRequestException;

    /**
     * Delete edges from named graph.
     *
     * @param application Name of application defining schema
     * @param transactionVisibility Visibility of delete transaction
     * @param graphName Name of graph
     * @param edges List of edges to delete
     * @param authorizations Authorizations for transaction
     * @return Transaction id
     * @throws ezbake.services.graph.thrift.InvalidRequestException If schema definition has error
     */
    public TransactionId deleteEdges(
            String application, String transactionVisibility, String graphName, List<Edge> edges, String authorizations)
            throws InvalidRequestException;

    /**
     * Get vertex by id from named graph.
     *
     * @param graphName Name of graph
     * @param vertexId Id of requested vertex
     * @param authorizations Authorizations for transaction
     * @return Vertex with id, null if doesn't exist
     */
    public com.tinkerpop.blueprints.Vertex getVertex(String graphName, Object vertexId, String authorizations);

    /**
     * Get all vertices from named graph.
     *
     * @param graphName Name of graph
     * @param authorizations Authorizations for transaction
     * @return List of vertices
     */
    public Iterable<com.tinkerpop.blueprints.Vertex> getVertices(String graphName, String authorizations);

    /**
     * Find all vertices where the given property is equal to the given value. The property must be indexed under the
     * Standard index to be searchable by <code>findVertices</code>. This is a typed comparison and will take into
     * account the datatype of the property written. For inexact matches use <code> searchForVertices</code>.
     *
     * @param graphName Name of graph
     * @param key Property key to query
     * @param value Property value to match
     * @param authorizations Authorizations for transaction
     * @return List of matching vertices
     */
    public Iterable<com.tinkerpop.blueprints.Vertex> findVertices(
            String graphName, String key, Object value, String authorizations);

    /**
     * Find all vertices that match a given search query. In order for vertices to be searchable the properties must be
     * indexed under the Search index.
     *
     * @param graphName Name of graph
     * @param query Search query
     * @param authorizations Authorizations for transaction
     * @return List of matching vertices
     */
    public Iterable<com.tinkerpop.blueprints.Vertex> searchForVertices(
            String graphName, GraphQuery query, String authorizations, int limit);

    /**
     * Get a single edge by edge ID.
     *
     * @param graphName Name of graph
     * @param edgeId Id of requested edge
     * @param authorizations Authorizations for transaction
     * @return Edge with id, null if doesn't exist
     */
    public com.tinkerpop.blueprints.Edge getEdge(String graphName, Object edgeId, String authorizations);

    /**
     * Get all the edges from named graph.
     *
     * @param graphName Name of graph
     * @param authorizations Authorizations for transaction
     * @return List of edges
     */
    public Iterable<com.tinkerpop.blueprints.Edge> getEdges(String graphName, String authorizations);

    /**
     * Find edges where the given property is equal to the given value. The property must be indexed under the Standard
     * index to be searchable by <code> findEdges</code>. This is a typed comparison and will take into account the
     * datatype of the property written. For inexact matches use <code>searchForEdges</code>.
     *
     * @param graphName Name of graph
     * @param key Property key to query
     * @param value Property value to match
     * @param authorizations Authorizations for transaction
     * @return List of matching edges
     */
    public Iterable<com.tinkerpop.blueprints.Edge> findEdges(
            String graphName, String key, Object value, String authorizations);

    /**
     * Find all vertices that match a given search query. In order for vertices to be searchable the properties you must
     * be indexed under the Search index.
     *
     * @param graphName Name of graph
     * @param query Search query
     * @param authorizations Authorizations for transaction
     * @return List of matching edges
     */
    public Iterable<com.tinkerpop.blueprints.Edge> searchForEdges(
            String graphName, GraphQuery query, String authorizations, int limit);

    /**
     * Starting from a root vertex navigate outwards the given number of hops and return the subgraph found. The number
     * of hops cannot exceed a maximum value set in the implementation.
     *
     * @param graphName Name of graph
     * @param root Starting vertex for expand
     * @param maxHops Max number of hops
     * @param authorizations Authorizations for transaction
     * @return
     * @throws InvalidHopSizeException If max hops is invalid
     */
    public Graph expandSubgraph(String graphName, Vertex root, int maxHops, String authorizations)
            throws InvalidHopSizeException;

    /**
     * Starting at the start vertex, return the subgraph formed by finding the shortest path to the end vertex. The
     * maximum hops must be set to a value greater than 0 and less than a maximum set in the implementation.
     *
     * @param graphName Name of graph
     * @param startVertex Starting vertex for path
     * @param endVertex Ending vertex for path
     * @param maxHops Max number of hops
     * @param authorizations Authorizations for transaction
     * @return
     * @throws InvalidHopSizeException If max hops is invalid
     */
    public Graph findPath(String graphName, Vertex startVertex, Vertex endVertex, int maxHops, String authorizations)
            throws InvalidHopSizeException;

    public Iterable<com.tinkerpop.blueprints.Vertex> queryVertices(
            String graphName, Vertex startVertex, String gremlinQuery, String authorizations)
            throws InvalidQueryException;

    public Iterable<com.tinkerpop.blueprints.Edge> queryEdges(
            String graphName, Vertex startVertex, String gremlinQuery, String authorizations)
            throws InvalidQueryException;

    /**
     * Shut down graph store.
     */
    public void shutdown();
}
