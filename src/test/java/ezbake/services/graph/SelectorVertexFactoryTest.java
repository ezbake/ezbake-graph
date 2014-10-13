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

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloSecurityToken;
import com.thinkaurelius.titan.graphdb.secure.SecureGraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanGraph;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanTx;
import com.tinkerpop.blueprints.Vertex;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.common.graph.TitanGraphConfiguration;
import ezbake.services.graph.thrift.EzGraphServiceConstants;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Vector;

import static org.junit.Assert.*;

public class SelectorVertexFactoryTest {
    private static final String UNCLASS_KEY = "unclassKey";
    private static final String CSC_KEY = "cscKey";
    private static final String F2SIX_KEY = "42sixKey";
    private static final String SELECTOR_KEY = "selectorKey";
    private static final String TEST_KEY = "testKey";
    private static final int THREAD_COUNT = 100;
    private String[] vis = { "CSC", "42six", "", "CSC&42six", "CSC|42six" };
    private Random r = new Random(System.currentTimeMillis());

    /**
     * Tests that AddSelector method on the facade transaction. Ensures that only
     * one vertex is created when adding selectors of the same key/value with
     * different visibility.
     */
    @Test
    public void testAddSelector1() throws EzConfigurationLoaderException {
        SecureTitanGraph graph = getGraph();

        SelectorVertexFactory factory = new SelectorVertexFactory(graph);

        SecureTitanTx<AccumuloSecurityToken> tx = setWriteToken(graph, "CSC");
        Long id1 = factory.getSelectorId(SELECTOR_KEY, "111");

        TitanVertex v1 = tx.getVertex(id1);
        v1.addProperty(SELECTOR_KEY, "111");
        tx.commit();

        tx = setWriteToken(graph, "42six");
        Long id2 = factory.getSelectorId(SELECTOR_KEY, "111");

        TitanVertex v2 = tx.getVertex(id2);
        v2.addProperty(SELECTOR_KEY, "111");
        tx.commit();

        tx = setWriteToken(graph, "42six&CSC");
        Long id3 = factory.getSelectorId(SELECTOR_KEY, "111");

        TitanVertex v3 = tx.getVertex(id3);
        v3.addProperty(SELECTOR_KEY, "111");
        tx.commit();

        assertEquals(id1, id2);
        assertEquals(id1, id3);

        // ensure that only one vertex was created in the graph with that selector
        tx = setReadToken(graph, "");
        TitanVertex tv = tx.getVertex(id1);
        assertTrue(tv.getId().equals(id1));
        List<String> props = tv.getProperty(SELECTOR_KEY);
        assertTrue(props.size() == 0);
        tx.commit();

        tx = setReadToken(graph, "CSC");
        tv = tx.getVertex(id1);
        assertTrue(tv.getId().equals(id1));
        props = tv.getProperty(SELECTOR_KEY);
        assertTrue(props.size() == 1);
        tx.commit();

        tx = setReadToken(graph, "42six");
        tv = tx.getVertex(id2);
        assertTrue(tv.getId().equals(id2));
        props = tv.getProperty(SELECTOR_KEY);
        assertTrue(props.size() == 1);
        tx.commit();

        tx = setReadToken(graph, "42six,CSC");
        tv = tx.getVertex(id3);
        assertTrue(tv.getId().equals(id3));
        props = tv.getProperty(SELECTOR_KEY);
        assertTrue(props.size() == 3);
        tx.commit();

        graph.shutdown();
    }

    /**
     * Tests that adding the same selector with different values only results in one vertex created.
     */
    @Test
    public void testAddSelectorMultiThread() throws EzConfigurationLoaderException {
        int numThreads = THREAD_COUNT;

        final SecureTitanGraph<AccumuloSecurityToken> graph = getGraph();
        final SelectorVertexFactory factory = new SelectorVertexFactory(graph);
        Vector<Thread> threads = new Vector();

        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(new Runnable() {
                public void run() {
                    String visi = vis[r.nextInt(5)];
                    SecureTitanTx<AccumuloSecurityToken> tx = setWriteToken(graph, visi);
                    Long id = factory.getSelectorId(SELECTOR_KEY, "zzz");

                    TitanVertex v = tx.getVertex(id);
                    v.addProperty(SELECTOR_KEY, "zzz");
                    tx.commit();
                }
            });
            t.start();

            threads.add(t);
        }

        try {
            // join all threads to ensure we don't try to print any results out until all threads are done
            for (Thread t : threads) {
                t.join();
            }

            // get all vertices in the graph and make sure only one vertex has property 'zzz'
            Iterable<Vertex> vlist = graph.getVertices();
            int cnt = 0;
            for (Vertex v : vlist) {
                SecureTitanTx<AccumuloSecurityToken> tx = setReadToken(graph, "CSC,42six");
                Vertex vv = tx.getVertex(v.getId());
                List<String> values = vv.getProperty(SELECTOR_KEY);
                if(values.contains("zzz"))
                    cnt++;
                tx.commit();
            }

            assertEquals(1, cnt);
            graph.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    private SecureTitanTx<AccumuloSecurityToken> setWriteToken(SecureTitanGraph graph, String auth) {
        return graph.buildTransaction()
                .setWriteToken(AccumuloSecurityToken.getInstance().newWriteToken(auth))
                .start();
    }

    private SecureTitanTx<AccumuloSecurityToken> setReadToken(SecureTitanGraph graph, String auth) {
        return graph.buildTransaction()
                .setReadToken(AccumuloSecurityToken.getInstance().newReadToken(auth))
                .start();
    }

    /**
     * Returns a new graph instance with the Spring injected settings. All
     * settings are the same except for the authorizations which override the
     * Spring settings.
     *
     * @return graph
     */
    SecureTitanGraph<AccumuloSecurityToken> getGraph() throws EzConfigurationLoaderException {
        ClasspathConfigurationLoader loader = new ClasspathConfigurationLoader();
        TitanGraphConfiguration config = new TitanGraphConfiguration(loader.loadConfiguration());

        // create a new visibility titan graph
        SecureTitanGraph<AccumuloSecurityToken> graph =
                new SecureTitanGraph(new SecureGraphDatabaseConfiguration(config));

        // Setup labels and property key types. We need to do this because
        // autotyping is turned off in the graph so that multithreaded ingest
        // and visibility of edges can work.

        if (graph.getType("knows") == null)
            graph.makeLabel("knows").make();

        // unique constraint so that we can rely upon Titan's native uniqueness
        if (graph.getType(EzGraphServiceConstants.UNIQUE_KEY) == null)
            graph.makeKey(EzGraphServiceConstants.UNIQUE_KEY).indexed(Vertex.class)
                    .dataType(String.class).unique().single().make();

        // selector key needs to be a list key type so that multiple visibilities
        // of the same selector key/value
        // can be stored correctly.
        if (graph.getType(SELECTOR_KEY) == null)
            graph.makeKey(SELECTOR_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        if (graph.getType(UNCLASS_KEY) == null)
            graph.makeKey(UNCLASS_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        if (graph.getType(F2SIX_KEY) == null)
            graph.makeKey(F2SIX_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        if (graph.getType(CSC_KEY) == null)
            graph.makeKey(CSC_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        if (graph.getType(TEST_KEY) == null)
            graph.makeKey(TEST_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        graph.commit();

        return graph;
    }
}
