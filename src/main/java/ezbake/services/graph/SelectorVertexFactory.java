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

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloSecurityToken;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanGraph;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanTx;
import com.tinkerpop.blueprints.Vertex;

import ezbake.services.graph.thrift.EzGraphServiceConstants;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author edeprit
 */
public class SelectorVertexFactory {

    private final Logger log = LoggerFactory.getLogger(SelectorVertexFactory.class);

    public static final String UNIQUE_VIS = "_unique";

    /**
     * Used for thread safe operation when creating new unique vertices
     */
    private static final int addRetries = 5;

    /**
     * Delimiter used for creating unique value for key
     */
    private static final String SEPARATOR = "/";

    private final SecureTitanGraph<AccumuloSecurityToken> graph;

    public SelectorVertexFactory(SecureTitanGraph<AccumuloSecurityToken> graph) {
        this.graph = graph;
    }

    public Long getSelectorId(String selKey, String selVal) {
        String uniqueVal = selKey + SEPARATOR + selVal;
        Long vertexId = lookup(uniqueVal);
        if (vertexId == null) {
            vertexId = newVertex(uniqueVal);
        }

        return vertexId;
    }

    private Long lookup(String uniqueVal) {
        SecureTitanTx tx =
                graph.buildTransaction().setReadToken(AccumuloSecurityToken.getInstance().newReadToken(UNIQUE_VIS))
                        .start();

        Iterable<Vertex> itrbl = tx.getVertices(EzGraphServiceConstants.UNIQUE_KEY, uniqueVal);

        Long vid = null;
        if (!Iterables.isEmpty(itrbl)) {
            TitanVertex selectorVertex = (TitanVertex) Iterables.getOnlyElement(itrbl);
            vid = selectorVertex.getID();
        }

        return vid;
    }

    private Long newVertex(String uniqueVal) {
        Long vid = null;
        // try several times to add the new vertex if it doesn't already exist,
        // if a separate thread/process added it before this completes,
        // then titan's .unique() constraint would throw exception
        for (int i = 0; i < addRetries; i++) {
            if (vid != null) {
                return vid;
            } else {
                TitanVertex newVertex = null;
                try {
                    log.debug("Create new unique vertex: " + uniqueVal);
                    // create a new transaction that uses the unique visibility,
                    TitanTransaction tt = graph.newTransaction();
                    newVertex = tt.addVertex(null);
                    tt.commit();

                    tt = graph.buildTransaction().setWriteToken(new AccumuloSecurityToken().newWriteToken(UNIQUE_VIS))
                            .start();

                    newVertex = (TitanVertex) tt.getVertex(newVertex);
                    newVertex.setProperty(EzGraphServiceConstants.UNIQUE_KEY, uniqueVal);
                    tt.commit();
                    vid = newVertex.getID();
                } catch (Exception ex) {
                    log.debug(
                            "Exception creating unique vertex: " + uniqueVal + "\n" + ExceptionUtils.getStackTrace(ex));
                    // make sure to remove the vertex we just created if exception happened
                    if (newVertex != null) {
                        try {
                            graph.removeVertex(graph.getVertex(newVertex));
                            graph.commit();
                        } catch (Exception e) {
                        }
                    }
                } finally {
                    if (vid == null) {
                        vid = lookup(uniqueVal);
                    }
                }
            }
        }

        return vid;
    }
}
