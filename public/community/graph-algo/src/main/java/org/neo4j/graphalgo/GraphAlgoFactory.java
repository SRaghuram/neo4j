/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphalgo.impl.path.AStar;
import org.neo4j.graphalgo.impl.path.AllPaths;
import org.neo4j.graphalgo.impl.path.AllSimplePaths;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.path.DijkstraBidirectional;
import org.neo4j.graphalgo.impl.path.ExactDepthPathFinder;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;

import static org.neo4j.internal.helpers.MathUtil.DEFAULT_EPSILON;

/**
 * Static factory methods for the recommended implementations of common
 * graph algorithms for Neo4j. The algorithms exposed here are implementations
 * which are tested extensively and also scale on bigger graphs.
 */
@PublicApi
public abstract class GraphAlgoFactory
{
    /**
     * Returns an algorithm which can find all available paths between two
     * nodes. These returned paths can contain loops (i.e. a node can occur
     * more than once in any returned path).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     * {@link Relationship}s for each {@link Path}.
     * @param maxDepth the max {@link Path#length()} returned paths are
     * allowed to have.
     * @return an algorithm which finds all paths between two nodes.
     */
    public static PathFinder<Path> allPaths( EvaluationContext context, PathExpander expander, int maxDepth )
    {
        return new AllPaths( context, maxDepth, expander );
    }

    /**
     * Returns an algorithm which can find all simple paths between two
     * nodes. These returned paths cannot contain loops (i.e. a node cannot
     * occur more than once in any returned path).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     * {@link Relationship}s for each {@link Path}.
     * @param maxDepth the max {@link Path#length()} returned paths are
     * allowed to have.
     * @return an algorithm which finds simple paths between two nodes.
     */
    public static PathFinder<Path> allSimplePaths( EvaluationContext context, PathExpander expander, int maxDepth )
    {
        return new AllSimplePaths( context, maxDepth, expander );
    }

    /**
     * Returns an algorithm which can find all shortest paths (that is paths
     * with as short {@link Path#length()} as possible) between two nodes. These
     * returned paths cannot contain loops (i.e. a node cannot occur more than
     * once in any returned path).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     *            {@link Relationship}s for each {@link Path}.
     * @param maxDepth the max {@link Path#length()} returned paths are allowed
     *            to have.
     * @return an algorithm which finds shortest paths between two nodes.
     */
    public static PathFinder<Path> shortestPath( EvaluationContext context, PathExpander expander, int maxDepth )
    {
        return new ShortestPath( context, maxDepth, expander );
    }

    /**
     * Returns an algorithm which can find all shortest paths (that is paths
     * with as short {@link Path#length()} as possible) between two nodes. These
     * returned paths cannot contain loops (i.e. a node cannot occur more than
     * once in any returned path).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     *            {@link Relationship}s for each {@link Path}.
     * @param maxDepth the max {@link Path#length()} returned paths are allowed
     *            to have.
     * @param maxHitCount the maximum number of {@link Path}s to return.
     * If this number of found paths are encountered the traversal will stop.
     * @return an algorithm which finds shortest paths between two nodes.
     */
    public static PathFinder<Path> shortestPath( EvaluationContext context, PathExpander expander, int maxDepth, int maxHitCount )
    {
        return new ShortestPath( context, maxDepth, expander, maxHitCount );
    }

    /**
     * Returns an algorithm which can find simple all paths of a certain length
     * between two nodes. These returned paths cannot contain loops (i.e. a node
     * could not occur more than once in any returned path).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param length the {@link Path#length()} returned paths will have, if any
     * paths were found.
     * @return an algorithm which finds paths of a certain length between two nodes.
     */
    public static PathFinder<Path> pathsWithLength( EvaluationContext context, PathExpander expander, int length )
    {
        return new ExactDepthPathFinder( context, expander, length, Integer.MAX_VALUE, false );
    }

    /**
     * Returns an {@link PathFinder} which uses the A* algorithm to find the
     * cheapest path between two nodes. The definition of "cheap" is the lowest
     * possible cost to get from the start node to the end node, where the cost
     * is returned from {@code lengthEvaluator} and {@code estimateEvaluator}.
     * These returned paths cannot contain loops (i.e. a node cannot occur more
     * than once in any returned path).
     *
     * See http://en.wikipedia.org/wiki/A*_search_algorithm for more
     * information.
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding
     * {@link Relationship}s for each {@link Path}.
     * @param lengthEvaluator evaluator that can return the cost represented
     * by each relationship the algorithm traverses.
     * @param estimateEvaluator evaluator that returns an (optimistic)
     * estimation of the cost to get from the current node (in the traversal)
     * to the end node.
     * @return an algorithm which finds the cheapest path between two nodes
     * using the A* algorithm.
     */
    public static PathFinder<WeightedPath> aStar( EvaluationContext context, PathExpander expander,
            CostEvaluator<Double> lengthEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        return new AStar( context, expander, lengthEvaluator, estimateEvaluator );
    }

    /**
     * Returns a {@link PathFinder} which uses the Dijkstra algorithm to find
     * the cheapest path between two nodes. The definition of "cheap" is the
     * lowest possible cost to get from the start node to the end node, where
     * the cost is returned from {@code costEvaluator}. These returned paths
     * cannot contain loops (i.e. a node cannot occur more than once in any
     * returned path).
     *
     * Dijkstra assumes none negative costs on all considered relationships.
     * If this is not the case behaviour is undefined. Do not use Dijkstra
     * with negative weights or use a {@link CostEvaluator} that handles
     * negative weights.
     *
     * See http://en.wikipedia.org/wiki/Dijkstra%27s_algorithm for more
     * information.
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding {@link Relationship}s for each {@link Path}.
     * @param costEvaluator evaluator that can return the cost represented by each relationship the algorithm traverses.
     * @return an algorithm which finds the cheapest path between two nodes using the Dijkstra algorithm.
     */
    public static PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander, CostEvaluator<Double> costEvaluator )
    {
        return new DijkstraBidirectional( context, expander, costEvaluator, DEFAULT_EPSILON );
    }

    /**
     * See {@link #dijkstra(EvaluationContext, PathExpander, CostEvaluator)} for documentation.
     *
     * Uses a cost evaluator which uses the supplied property key to
     * represent the cost (values of type <b>double</b>).
     *
     * @param context algorithm evaluation context
     * @param expander the {@link PathExpander} to use for expanding {@link Relationship}s for each {@link Path}.
     * @param relationshipPropertyRepresentingCost the property to represent cost on each relationship the algorithm traverses.
     * @return an algorithm which finds the cheapest path between two nodes using the Dijkstra algorithm.
     */
    public static PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander, String relationshipPropertyRepresentingCost )
    {
        return dijkstra( context, expander, new DoubleEvaluator( relationshipPropertyRepresentingCost ) );
    }

    /**
     * See {@link #dijkstra(EvaluationContext, PathExpander, CostEvaluator)} for documentation
     *
     * Instead of finding all shortest paths with equal cost, find the top {@code numberOfWantedPaths} paths.
     * This is usually slower than finding all shortest paths with equal cost.
     *
     * Uses a cost evaluator which uses the supplied property key to
     * represent the cost (values of type <b>double</b>).
     *
     * @param expander the {@link PathExpander} to use for expanding {@link Relationship}s for each {@link Path}.
     * @param relationshipPropertyRepresentingCost the property to represent cost on each relationship the algorithm traverses.
     * @param numberOfWantedPaths number of paths to find.
     * @return an algorithm which finds the cheapest path between two nodes using the Dijkstra algorithm.
     */
    public static PathFinder<WeightedPath> dijkstra( PathExpander<Double> expander, String relationshipPropertyRepresentingCost, int numberOfWantedPaths )
    {
        return dijkstra( expander, new DoubleEvaluator( relationshipPropertyRepresentingCost ), numberOfWantedPaths );
    }

    /**
     * See {@link #dijkstra(EvaluationContext, PathExpander, CostEvaluator)} for documentation
     *
     * Instead of finding all shortest paths with equal cost, find the top {@code numberOfWantedPaths} paths.
     * This is usually slower than finding all shortest paths with equal cost.
     *
     * @param expander the {@link PathExpander} to use for expanding {@link Relationship}s for each {@link Path}.
     * @param costEvaluator evaluator that can return the cost represented by each relationship the algorithm traverses.
     * @param numberOfWantedPaths number of paths to find.
     * @return an algorithm which finds the cheapest path between two nodes using the Dijkstra algorithm.
     */
    public static PathFinder<WeightedPath> dijkstra( PathExpander<Double> expander, CostEvaluator<Double> costEvaluator, int numberOfWantedPaths )
    {
        return new Dijkstra( expander, costEvaluator, DEFAULT_EPSILON, PathInterestFactory.numberOfShortest( DEFAULT_EPSILON, numberOfWantedPaths ) );
    }
}
