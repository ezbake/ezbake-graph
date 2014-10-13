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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.common.graph.GraphConverter;
import ezbake.data.test.TestUtils;
import ezbake.services.graph.archive.MockTransactionArchive;
import ezbake.services.graph.thrift.GraphName;
import ezbake.services.graph.thrift.InvalidRequestException;
import ezbake.services.graph.thrift.types.DataType;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.EdgeLabel;
import ezbake.services.graph.thrift.types.Element;
import ezbake.services.graph.thrift.types.ElementId;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.Index;
import ezbake.services.graph.thrift.types.IndexName;
import ezbake.services.graph.thrift.types.PropValue;
import ezbake.services.graph.thrift.types.Property;
import ezbake.services.graph.thrift.types.PropertyKey;
import ezbake.services.graph.thrift.types.TitanId;
import ezbake.services.graph.thrift.types.Vertex;

public class GraphDataSetHandlerTest {
    /**
     * Values used for setting up a selector vertex that can be used for tests.
     */
    private static final String SELECTOR_PROP_VALUE_1 = "111";
    private static final String FIRST_SELECTOR_ID = "v1";
    private static final String SELECTOR_PROP_VALUE_2 = "n111";
    private static final String SECOND_SELECTOR_ID = "n1";

    /**
     * Required for creating schemas and writing graphs - ultimately stored in the backend with the rest of the
     * transaction. Also may be used as the selector property key.
     */
    private static final String APPLICATION_NAME = "foobarApp";
    /**
     * The KEY_TO_SELECTOR_PROP is used as the key to the selector property. This could be the application name, or any
     * property whose value should be guaranteed unique.
     */
    private static final String KEY_TO_SELECTOR_PROP = APPLICATION_NAME;
    /**
     * "KEY_TO_<visibility>_PROP" members are meant to indicate that the property they are a part of should be assigned
     * a visiblity of <visibility>. The name of the key does not confer any visiblity to the property.
     */
    private static final String KEY_TO_UNCLASS_PROP = "U";
    private static final String KEY_TO_SECRET_PROP = "S";
    private static final String KEY_TO_TOPSECRET_PROP = "TS";
    /**
     * Arbitrary label for an edge. "knows" might be the kind of label used when EzGraph'ing social data.
     */
    private static final String EDGE_LABEL = "knows";

    /**
     * Preset Visibility object types
     */
    private static final Visibility secretVisibility = new Visibility();
    private static final Visibility unclassVisibility = new Visibility();
    private static final Visibility topSecretVisibility = new Visibility();
    /**
     * Visibility used for writing schemas and graphs.
     */
    private static final Visibility transactionVisibility = topSecretVisibility;
    /**
     * Preset EzSecurityTokens
     */
    private static final EzSecurityToken TS_Auth_Token;
    private static final EzSecurityToken S_Auth_Token;
    private static final EzSecurityToken U_Auth_Token;
    /**
     * May help in guaranteeing graph name uniqueness/prevent against test bleed.
     */
    private static int cnt;

    /**
     * Initialize the visibility objects and EzSecurity token objects.
     */
    static {
        secretVisibility.setFormalVisibility("S");
        topSecretVisibility.setFormalVisibility("TS");
        unclassVisibility.setFormalVisibility("U");
        TS_Auth_Token = TestUtils.createTestToken(new String[] {"S", "U", "TS", "USA"});
        S_Auth_Token = TestUtils.createTestToken(new String[] {"S", "U", "USA"});
        U_Auth_Token = TestUtils.createTestToken(new String[] {"U"});
    }

    /**
     * 'Offline' thrift interface for EzGraph.
     */
    private GraphDataSetHandler handler;

    /**
     * Used to specify the graph to be written to. If no graph name is used then methods like 'createSchema(...)' and
     * 'writeGraph(...)' will affect the default/global graph.
     */
    private GraphName graphName;

    /**
     * Convenience method that makes a property key with the specified name.
     *
     * @param name What the key will be named.
     * @return The newly created key.
     */
    private static PropertyKey makePropertyKey(String name) {
        return new PropertyKey(name);
    }

    private static PropertyKey makeIndexedKey(String name) {
        final Index index = new Index(Element.VERTEX).setIndexName(IndexName.SEARCH);
        return makePropertyKey(name).setIndices(Lists.newArrayList(index));
    }

    private static PropertyKey makeStringKey(String name) {
        return makePropertyKey(name).setDataType(DataType.STRING);
    }

    private static PropertyKey makeIndexedStringKey(String name) {
        return makeIndexedKey(name).setDataType(DataType.STRING);
    }

    @SuppressWarnings("unused")
    private static PropertyKey makeIntegerKey(String name) {
        return makePropertyKey(name).setDataType(DataType.I32);
    }

    private static PropertyKey makeIndexedIntegerKey(String name) {
        return makeIndexedKey(name).setDataType(DataType.I32);
    }

    @Before
    public void setUp() throws EzConfigurationLoaderException {
        // External dependency on file src/test/resources/ezbake-config.properties,
        // properties will typically be retrieved using 'new EzConfiguration()';
        final ClasspathConfigurationLoader loader = new ClasspathConfigurationLoader();
        final Properties properties = loader.loadConfiguration();

        TestUtils.addSettingsForMock(properties);

        handler = new GraphDataSetHandler(new MockTransactionArchive());
        handler.setConfigurationProperties(properties);
        handler.getThriftProcessor(); // calls init();

        // may protect against bleed between tests...
        graphName = getGraphName();
    }

    @After
    public void tearDown() throws Exception {
        handler.shutdown();
    }

    /**
     * Tests that vertex property keys and edge labels can be created. The security token has no effect during schema
     * creation.
     */
    @Test
    public void testCreateSchema() throws InvalidRequestException, TException {
        // test normal operation
        handler.createSchema(
                APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);
        // writes to 'global graph' when no graph name is present
        handler.createSchema(
                APPLICATION_NAME, transactionVisibility, null, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);

        try {
            handler.createSchema(
                    null, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);
            fail("Parameters not properly validated");
        } catch (final Exception e) {
            // expected exception
        }

        try {
            handler.createSchema(
                    APPLICATION_NAME, transactionVisibility, null, getPropertyKeys(), getEdgeLabels(), null);
            fail("Token parameters not properly checked");
        } catch (final Exception e) {
            // expected exception
        }

        try {
            graphName = getGraphName();
            handler.createSchema(
                    APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(),
                    U_Auth_Token);
            handler.createSchema(
                    APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(),
                    U_Auth_Token);
            fail("Was able to write the same schema twice.");
        } catch (final Exception e) {
            // expected exception
        }

        // test normal operation for adding new keys to schema
        final ArrayList<PropertyKey> plist = new ArrayList<>();
        plist.add(new PropertyKey("foobar-vert"));

        final ArrayList<EdgeLabel> elist = new ArrayList<>();
        elist.add(new EdgeLabel("foobar-edge"));

        graphName = getGraphName();
        handler.createSchema(
                APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);
        handler.createSchema(APPLICATION_NAME, transactionVisibility, graphName, plist, elist, U_Auth_Token);
    }

    /**
     * Make sure exception is thrown if no schema was created prior to other graph calls.
     *
     * @throws TException Not applicable because this is not run over thrift.
     */
    @Test
    public void testWriteGraphNoSchema() throws Exception {
        try {
            handler.writeGraph(APPLICATION_NAME, transactionVisibility, graphName, createThriftGraph(), U_Auth_Token);
            fail();
        } catch (final InvalidRequestException ex) {
            assertTrue(
                    "Exception did not have expected message.",
                    ex.getMessage().contains("User attempted to write a graph before creating a schema"));
        }
    }

    @Test
    public void testWriteGraph() throws Exception {
        handler.createSchema(
                APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);
        handler.writeGraph(APPLICATION_NAME, transactionVisibility, graphName, createThriftGraph(), U_Auth_Token);

        handler.writeGraph(APPLICATION_NAME, transactionVisibility, null, createThriftGraph(), U_Auth_Token);
    }

    @Test
    public void testFindVertices() throws Exception {
        prepareNewGraph();

        List<Vertex> vlist = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token);
        assertEquals("Find vertices retrieved did not retrieve the expected number", 1, vlist.size());
        assertEquals(
                "Found vertex had unexpected or missing property value.", 10,
                vlist.get(0).getProperties().get(KEY_TO_TOPSECRET_PROP).get(0).getValue().getI32_val());

        vlist = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), S_Auth_Token);
        assertEquals(
                "Find vertices retrieved at least one vertex it should not have been able to find based on visibility",
                0, vlist.size());
    }

    /**
     * The selector property allows us a way to guarantee the uniqueness of any vertex when this property set. Only one
     * vertex can be created with the value corresponding to the selector property. As such, we should be able to create
     * multiple vertices and retrieve them based on their selector property.
     * <p/>
     * This method uses printVertex(...) as a convenience to make potential output easier to read.
     */
    @Test
    public void testMultipleSelectors() throws InvalidRequestException, TException {
        prepareNewGraph(createSelectorVertex(SECOND_SELECTOR_ID, SELECTOR_PROP_VALUE_2, topSecretVisibility, null));

        List<Vertex> vlist = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token);
        assertEquals(1, vlist.size());
        final Vertex firstVertex = vlist.get(0);
        // TODO: remove when the selector property is being set on retrieval:
        firstVertex.setSelectorProperty(KEY_TO_SELECTOR_PROP);
        assertEquals(
                printVertex(createSelectorVertex(FIRST_SELECTOR_ID, SELECTOR_PROP_VALUE_1, topSecretVisibility, 4L)),
                printVertex(firstVertex));

        vlist = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_2), TS_Auth_Token);
        assertEquals(1, vlist.size());
        final Vertex secondVertex = vlist.get(0);
        // TODO: remove when the selector property is being set on retrieval:
        secondVertex.setSelectorProperty(KEY_TO_SELECTOR_PROP);
        assertEquals(
                printVertex(createSelectorVertex(SECOND_SELECTOR_ID, SELECTOR_PROP_VALUE_2, topSecretVisibility, 8L)),
                printVertex(secondVertex));

        assertNotEquals(
                "Vertices were the same, but should have been different.", printVertex(firstVertex),
                printVertex(secondVertex));
    }

    @Test
    public void testFindEdges() throws InvalidRequestException, TException {
        prepareNewGraph();

        final List<Edge> elist =
                handler.findEdges(graphName, KEY_TO_UNCLASS_PROP, GraphConverter.convertObject("e4"), TS_Auth_Token);

        assertEquals("Retrieved an unexpected number of edges.", 1, elist.size());
        assertEquals(
                "Retrieved edge property value did not match expected", "e4",
                elist.get(0).getProperties().get(KEY_TO_UNCLASS_PROP).getValue().getString_val());
    }

    // TODO: Add tests specifically for testing visibility

    // TODO: Add tests for testing object conversion (in a new test class?)

    // TODO: Add tests for unsupported operation exception?

    @Test
    public void testGetEdge() throws InvalidRequestException, TException {
        prepareNewGraph();

        final List<Edge> elist = handler.getEdges(graphName, U_Auth_Token);
        final Edge e = elist.get(0);
        final ElementId eid = e.getId();
        final Edge e2 = handler.getEdge(graphName, eid, U_Auth_Token);

        assertEquals(
                "Retrieved edge did not match expected edge.", e2.getId().getTitanId().getEdgeId(),
                e.getId().getTitanId().getEdgeId());
    }

    @Test
    public void testGetVertex() throws InvalidRequestException, TException {
        prepareNewGraph();

        final List<Vertex> list = handler.getVertices(graphName, U_Auth_Token);
        final Vertex v = list.get(0);
        final ElementId vid = v.getId();
        final Vertex v2 = handler.getVertex(graphName, vid, U_Auth_Token);

        // Using printVertex(...) to make potential output easier to read.
        assertEquals("Get vertex did not return expected vertex.", printVertex(v), printVertex(v2));
    }

    @Test
    public void testExpandSubgraph() throws InvalidRequestException, TException {
        prepareNewGraph();
        int numberOfHops = 0;

        final Vertex rootVertex = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token)
                .get(0);
        Graph thriftSubgraph = handler.expandSubgraph(graphName, rootVertex, numberOfHops, TS_Auth_Token);
        List<Vertex> vlist = thriftSubgraph.getVertices();
        // Using printVertex(...) to make potential output easier to read.
        assertEquals(
                "Expanding a subgraph 0 hops did not return expected vertex", printVertex(rootVertex),
                printVertex(vlist.get(0)));

        numberOfHops = 1;
        thriftSubgraph = handler.expandSubgraph(graphName, rootVertex, numberOfHops, TS_Auth_Token);
        vlist = thriftSubgraph.getVertices();
        checkArraysEqualWhenSorted(
                new long[] {8, 4, 20, 12}, getSortedTitanIds(vlist),
                "Expanding subgraph once did not return expected vertices");

        numberOfHops = 2;
        thriftSubgraph = handler.expandSubgraph(graphName, rootVertex, numberOfHops, TS_Auth_Token);
        vlist = thriftSubgraph.getVertices();
        checkArraysEqualWhenSorted(
                new long[] {8, 4, 20, 12, 16, 24}, getSortedTitanIds(vlist),
                "Expanding subgraph twice did not return expected vertices");

        numberOfHops = 3;
        thriftSubgraph = handler.expandSubgraph(graphName, rootVertex, numberOfHops, TS_Auth_Token);
        vlist = thriftSubgraph.getVertices();
        checkArraysEqualWhenSorted(
                new long[] {8, 4, 28, 20, 12, 16, 24}, getSortedTitanIds(vlist),
                "Expanding subgraph three times did not return expected");
    }

    @Test
    public void testFindPath() throws InvalidRequestException, TException {
        prepareNewGraph();

        final Vertex startVertex = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token)
                .get(0);
        final Vertex endVertex = handler.findVertices(
                graphName, KEY_TO_UNCLASS_PROP, GraphConverter.convertObject("v6"), TS_Auth_Token).get(0);
        final int maxHops = 1;

        // TODO check maxHops param robust?
        final Graph thriftSubgraph = handler.findPath(graphName, startVertex, endVertex, maxHops, TS_Auth_Token);
        final List<Vertex> vlist = thriftSubgraph.getVertices();

        checkArraysEqualWhenSorted(
                new long[] {8, 4, 20, 12}, getSortedTitanIds(vlist), "Find path did not return expected vertices");
    }

    // TODO: document some notes on how indexing works (search vs standard, Index(Element.Vertex) vs? ).

    @Test
    public void testQueryVertices() throws InvalidRequestException, TException {
        prepareNewGraph(createSelectorVertex(SECOND_SELECTOR_ID, SELECTOR_PROP_VALUE_2, topSecretVisibility, null));
        Vertex startVertex = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token)
                .get(0);
        List<Vertex> vlist = handler.queryVertices(graphName, startVertex, "_().out('knows')", TS_Auth_Token);
        checkArraysEqualWhenSorted(
                new long[] {12, 16, 24}, getSortedTitanIds(vlist),
                "Vertex query on selector vertex did not return expected vertices.");

        startVertex = handler.findVertices(
                graphName, KEY_TO_UNCLASS_PROP, GraphConverter.convertObject("v6"), TS_Auth_Token).get(0);
        vlist = handler.queryVertices(graphName, startVertex, "_().out('knows')", TS_Auth_Token);
        checkArraysEqualWhenSorted(
                new long[] {32}, getSortedTitanIds(vlist),
                "Vertex query on non-selector-vertex did not return expected vertices.");

        startVertex = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_2), TS_Auth_Token)
                .get(0);
        vlist = handler.queryVertices(graphName, startVertex, "_().out('knows')", TS_Auth_Token);
        checkArraysEqualWhenSorted(
                new long[] {}, getSortedTitanIds(vlist),
                "Vertex query on vertex with no edges did not return expected vertices (none).");
    }

    @Test
    public void testQueryEdges() throws InvalidRequestException, TException {
        prepareNewGraph();
        final Vertex startVertex = handler.findVertices(
                graphName, KEY_TO_SELECTOR_PROP, GraphConverter.convertObject(SELECTOR_PROP_VALUE_1), TS_Auth_Token)
                .get(0);
        final List<Edge> elist = handler.queryEdges(graphName, startVertex, "_().outE()", TS_Auth_Token);
        final List<String> expected = Lists.newArrayList("e1", "e2", "e4");

        assertTrue("Query did not return expected number of edges.", expected.size() == elist.size());

        final StringBuilder foundPropVals = new StringBuilder();
        boolean fail = false;
        for (final Edge e : elist) {
            final String edgePropValue = e.getProperties().get(KEY_TO_UNCLASS_PROP).getValue().getString_val();
            if (!expected.contains(edgePropValue)) {
                fail = true;
            }
            foundPropVals.append(edgePropValue + " ");
        }
        if (fail) {
            fail(
                    "Did not receive expected edges from query. Expected: " + Arrays.toString(expected.toArray())
                            + " found: " + foundPropVals);
        }
    }

    /**
     * Edge labels need to be defined in the schema.
     *
     * @return A list of edge labels that can be used when writing the schema.
     */
    List<EdgeLabel> getEdgeLabels() {
        final List<EdgeLabel> labels = new ArrayList<>();
        labels.add(new EdgeLabel("knows"));
        return labels;
    }

    /**
     * Sorts two long arrays and performs an assertEquals on them.
     *
     * @param expected Expected values for assertArrayEquals(...).
     * @param actual Actual values for assertArrayEquals(...).
     * @param errorMsg Error message to display if sorted arrays are NOT equal.
     */
    private void checkArraysEqualWhenSorted(long[] expected, long[] actual, String errorMsg) {
        Arrays.sort(expected);
        Arrays.sort(actual);
        assertArrayEquals(errorMsg, expected, actual);
    }

    /**
     * Extracts TitanIds from a list of vetices and sorts them for easy comparison.
     *
     * @param vlist A list of vertices retrieved from EzGraph.
     * @return A sorted long[] populated by the TitanIds from the list of vertices.
     */
    private long[] getSortedTitanIds(List<Vertex> vlist) {
        final long[] ids = new long[vlist.size()];
        for (int i = 0; i < vlist.size(); i++) {
            ids[i] = vlist.get(i).getId().getTitanId().getVertexId();
        }

        return ids;
    }

    /**
     * Convenience method for creating schemas. Property keys written to a schema can be used as keys on properties in
     * vertices and edges written to a graph with this schema.
     *
     * @return A list of property keys that can be used to write a schema.
     */
    private List<PropertyKey> getPropertyKeys() {
        final List<PropertyKey> pkeys = new ArrayList<>();
        pkeys.add(makeStringKey(KEY_TO_SELECTOR_PROP));
        pkeys.add(makeIndexedStringKey(KEY_TO_UNCLASS_PROP));
        pkeys.add(makeIndexedIntegerKey(KEY_TO_TOPSECRET_PROP));
        pkeys.add(makeIndexedStringKey(KEY_TO_SECRET_PROP));

        return pkeys;
    }

    /**
     * Returns a new graph name
     */
    private GraphName getGraphName() {
        cnt++;
        final GraphName gn = new GraphName();
        gn.setName("graph_" + cnt);
        return gn;
    }

    /**
     * Creates a map containing one property that can be added to an edge.
     *
     * @param value The value of the property this map contains.
     * @return The completed map with property.
     */
    private Map<String, Property> getEdgeProperties(String value) {
        final Map<String, Property> map = Maps.newHashMap();
        map.put(KEY_TO_UNCLASS_PROP, GraphConverter.convertProperty(value));
        return map;
    }

    /**
     * Creates a map containing lists of properties with different visibilities.
     *
     * @param localId The value for an unclassified property in one of the property lists.
     * @return The completed map with several property lists.
     */
    private Map<String, List<Property>> getVertexProperties(String localId) {
        final Map<String, List<Property>> map = Maps.newHashMap();

        Property p = new Property();
        p.setValue(GraphConverter.convertObject("secret value"));
        p.setVisibility(secretVisibility);
        map.put(KEY_TO_SECRET_PROP, Lists.newArrayList(p));

        p = new Property();
        p.setVisibility(topSecretVisibility);
        p.setValue(GraphConverter.convertObject(new Integer(10)));
        map.put(KEY_TO_TOPSECRET_PROP, Lists.newArrayList(p));

        p = new Property();
        p.setVisibility(unclassVisibility);
        p.setValue(GraphConverter.convertObject(localId));
        map.put(KEY_TO_UNCLASS_PROP, Lists.newArrayList(p));

        return map;
    }

    /**
     * @param id The value that will be used for the local id (necessary for writes) as well as added as an additional
     * property to the vertex.
     * @param selectorValue The value to be assigned the selector property.
     * @param visibility The visibility of the created Vertex.
     * @param titanId Set this if you want to create a vertex for comparison to vertex returned by a query. Do not set
     * if you are trying to write to a graph.
     * @return The new selector vertex.
     */
    private Vertex createSelectorVertex(String id, String selectorValue, Visibility visibility, Long titanId) {
        final Vertex selectorVertex = new Vertex();
        final ElementId vid1 = new ElementId();
        if (titanId == null) {
            vid1.setLocalId(id);
        } else {
            final TitanId tId = new TitanId();
            tId.setVertexId(titanId);
            vid1.setTitanId(tId);
        }

        selectorVertex.setId(vid1);

        final Map<String, List<Property>> props = getVertexProperties(id);
        final Property selProp = new Property();
        final PropValue pv = new PropValue();
        pv.setString_val(selectorValue);
        selProp.setValue(pv);
        selProp.setVisibility(visibility);
        selectorVertex.setSelectorProperty(KEY_TO_SELECTOR_PROP);

        props.put(KEY_TO_SELECTOR_PROP, Lists.newArrayList(selProp));

        selectorVertex.setProperties(props);

        return selectorVertex;
    }

    /**
     * Writes a schema and graph to Titan as well as any additional vertices passed in.
     *
     * @param additionalVerts Additional vertices to add the created graph.
     * @throws InvalidRequestException If invalid input is sent to the handler.
     * @throws TException Not applicable unless running on thrift.
     */
    private void prepareNewGraph(Vertex... additionalVerts) throws InvalidRequestException, TException {
        handler.createSchema(
                APPLICATION_NAME, transactionVisibility, graphName, getPropertyKeys(), getEdgeLabels(), U_Auth_Token);
        handler.writeGraph(
                APPLICATION_NAME, transactionVisibility, graphName, createThriftGraph(additionalVerts), U_Auth_Token);
    }

    /**
     * Creates a graph with at least a single selector vertex, several non-selector vertices, and several edges.
     *
     * @param additionalVerts Optionally include additional vertices.
     * @return A graph that can be written to EzGraph, or further modified.
     */
    private Graph createThriftGraph(Vertex... additionalVerts) {
        final Graph thriftGraph = new Graph();

        final Map<String, Vertex> vmap = new HashMap<>();

        // create a selector Vertex
        final String selId = FIRST_SELECTOR_ID;
        final String selValue = SELECTOR_PROP_VALUE_1;
        final Vertex selectorVertex = createSelectorVertex(selId, selValue, topSecretVisibility, null);
        thriftGraph.addToVertices(selectorVertex);

        vmap.put(selId, selectorVertex);

        for (final Vertex addtl : additionalVerts) {
            vmap.put(addtl.getId().getLocalId(), addtl);
            thriftGraph.addToVertices(addtl);
        }

        for (int i = 2; i <= 7; i++) {
            final String vId = "v" + i;
            final Vertex v = createVertex(vId);
            vmap.put(vId, v);
            thriftGraph.addToVertices(v);
        }

        thriftGraph.addToEdges(
                createEdge(
                        "e1", unclassVisibility, vmap.get(FIRST_SELECTOR_ID).getId(), vmap.get("v2").getId()));
        thriftGraph.addToEdges(
                createEdge(
                        "e2", unclassVisibility, vmap.get(FIRST_SELECTOR_ID).getId(), vmap.get("v3").getId()));
        thriftGraph.addToEdges(createEdge("e3", unclassVisibility, vmap.get("v3").getId(), vmap.get("v4").getId()));
        thriftGraph.addToEdges(
                createEdge(
                        "e4", unclassVisibility, vmap.get(FIRST_SELECTOR_ID).getId(), vmap.get("v5").getId()));
        thriftGraph.addToEdges(createEdge("e5", unclassVisibility, vmap.get("v5").getId(), vmap.get("v6").getId()));
        thriftGraph.addToEdges(createEdge("e6", unclassVisibility, vmap.get("v6").getId(), vmap.get("v7").getId()));
        thriftGraph.addToEdges(
                createEdge(
                        "e7", unclassVisibility, vmap.get("v3").getId(), vmap.get(FIRST_SELECTOR_ID).getId()));

        return thriftGraph;
    }

    /**
     * Creates an edge with the passed in properties. Edges connect two vertices and have directionality based on the
     * 'out vertex' and the 'in vertex'.
     *
     * @param localId Id requirted for writing to the graph.
     * @param visibility Security property that determines who can see an edge in queries.
     * @param outVid The Id of the 'out' vertex.
     * @param inVid The Id of the 'in' vertex.
     * @return The newly created edge.
     */
    private Edge createEdge(String localId, Visibility visibility, ElementId outVid, ElementId inVid) {
        final Edge edge = new Edge();
        final ElementId eid = new ElementId();
        edge.setId(eid);
        edge.setProperties(getEdgeProperties(localId));
        edge.setLabel(EDGE_LABEL);
        edge.setVisibility(visibility);

        edge.setOutVertex(outVid);
        edge.setInVertex(inVid);
        return edge;
    }

    /**
     * Creates a vertex without a selector property.
     *
     * @param localId required for writing a graph, not persisted.
     * @return A newly created vertex.
     */
    private Vertex createVertex(String localId) {
        final Vertex v = new Vertex();
        final ElementId vid = new ElementId();
        vid.setLocalId(localId);
        v.setProperties(getVertexProperties(localId));
        v.setId(vid);
        return v;
    }

    /**
     * Calls printVertex(...) once for each vertex passsed in.
     *
     * @param list A list of vertices to print.
     * @return A StringBuilder with information on each vertex appended to it.
     */
    @SuppressWarnings("unused")
    private StringBuilder printVertices(List<Vertex> list) {
        final StringBuilder vertices = new StringBuilder();

        for (final Vertex v : list) {
            vertices.append(printVertex(v));
        }
        return vertices;
    }

    /**
     * Prints out all attributes of a vertex and also returns them in a string. TODO: move printing of string to
     * individual tests. Consider printing the thrift object directly in some cases.
     * <p/>
     * Attempts to make output of tests and printing vertices easier to read.
     *
     * @param v The vertex to print.
     * @return A string representing all qualities of the vertex.
     */
    private String printVertex(Vertex v) {
        final StringBuilder output = new StringBuilder();
        output.append(System.getProperty("line.separator"));
        output.append("Vertex: " + v.getId().getTitanId().getVertexId());
        output.append(System.getProperty("line.separator"));
        output.append("Vertex Selector Property: " + v.getSelectorProperty());
        output.append(System.getProperty("line.separator"));

        for (final Map.Entry<String, List<Property>> property : v.getProperties().entrySet()) {
            final String key = property.getKey();
            final List<Object> values = Lists.newArrayList(
                    Iterables.transform(
                            property.getValue(), new Function<Property, Object>() {

                                @Override
                                public Object apply(Property p) {
                                    return GraphConverter.getJavaPropValue(p.getValue());
                                }
                            }));
            output.append("\t" + key + " : " + values);
            output.append(System.getProperty("line.separator"));
        }

        return output.toString();
    }
}
