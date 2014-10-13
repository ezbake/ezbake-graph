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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import ezbake.data.common.graph.GraphConverter;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.Graph;

public class EzBreadthFirstSearch {

    public static Graph bfs(
            Vertex rootVertex, int maxHops, Function<Vertex, Boolean> processor) {
        Queue<Vertex> workingVertices = new LinkedBlockingQueue<>();
        Queue<Vertex> nextVertices = new LinkedBlockingQueue<>();

        final Set<Edge> edges = new HashSet<>();
        final Set<ezbake.services.graph.thrift.types.Vertex> vertices = new HashSet<>();

        final Map<Object, Boolean> visited = new HashMap<>(); // vertex id => has been visited

        workingVertices.add(rootVertex);

        Vertex current;

        final StringBuffer anc = new StringBuffer("");

        anc.append(rootVertex.getId().toString() + ", ");

        for (int i = 0; i <= maxHops; i++) {
            vertices.addAll(GraphConverter.convertVertices(workingVertices));

            // while there are still vertices in the queue
            while ((current = workingVertices.poll()) != null) {
                // while end condition not reached
                if (!processor.apply(current)) {
                    break;
                }

                visited.put(current.getId(), true);

                // add all the connected vertices to current vertex minus the the ones we've already visited
                nextVertices.addAll(Lists.newArrayList(removeVisited(current.getVertices(Direction.OUT), visited)));

                // add all the edges from current vertex
                edges.addAll(GraphConverter.convertEdges(current.getEdges(Direction.OUT)));
            }

            // Swap references to the working set and the next set
            final Queue<Vertex> temp = workingVertices;
            workingVertices = nextVertices;
            nextVertices = temp;
        }

        final Graph outputGraph = new Graph();
        outputGraph.setVertices(new ArrayList<ezbake.services.graph.thrift.types.Vertex>(vertices));
        outputGraph.setEdges(new ArrayList<>(edges));
        return outputGraph;
    }

    private static Iterable<Vertex> removeVisited(
            Iterable<Vertex> collection, final Map<Object, Boolean> visited) {
        return Iterables.filter(
                collection, new com.google.common.base.Predicate<Vertex>() {
                    @Override
                    public boolean apply(Vertex vertex) {
                        // return true that the passed in vertex is not in the visited list
                        return visited.get(vertex.getId()) == null;
                    }
                });
    }
}
