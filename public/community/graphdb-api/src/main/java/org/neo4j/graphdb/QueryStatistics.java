/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb;

import org.neo4j.annotations.api.PublicApi;

/**
 * Represents statistics about the effects of a query.
 *
 * If the query did not perform any {@link #containsUpdates() updates}, all the methods of this interface will return
 * {@code 0}.
 */
@PublicApi
public interface QueryStatistics
{
    // NOTE: If you change this interface, be sure to update bolt

    /**
     * Returns the number of nodes created by this query.
     *
     * @return the number of nodes created by this query.
     */
    int getNodesCreated();

    /**
     * Returns the number of nodes deleted by this query.
     *
     * @return the number of nodes deleted by this query.
     */
    int getNodesDeleted();

    /**
     * Returns the number of relationships created by this query.
     *
     * @return the number of relationships created by this query.
     */
    int getRelationshipsCreated();

    /**
     * Returns the number of relationships deleted by this query.
     *
     * @return the number of relationships deleted by this query.
     */
    int getRelationshipsDeleted();

    /**
     * Returns the number of properties set by this query. Setting a property to the same value again still counts
     * towards this.
     *
     * @return the number of properties set by this query.
     */
    int getPropertiesSet();

    /**
     * Returns the number of labels added to any node by this query.
     *
     * @return the number of labels added to any node by this query.
     */
    int getLabelsAdded();

    /**
     * Returns the number of labels removed from any node by this query.
     *
     * @return the number of labels removed from any node by this query.
     */
    int getLabelsRemoved();

    /**
     * Returns the number of indexes added by this query.
     *
     * @return the number of indexes added by this query.
     */
    int getIndexesAdded();

    /**
     * Returns the number of indexes removed by this query.
     *
     * @return the number of indexes removed by this query.
     */
    int getIndexesRemoved();

    /**
     * Returns the number of constraints added by this query.
     *
     * @return the number of constraints added by this query.
     */
    int getConstraintsAdded();

    /**
     * Returns the number of constraints removed by this query.
     *
     * @return the number of constraints removed by this query.
     */
    int getConstraintsRemoved();

    /**
     * Returns the number of system updates performed by this query.
     *
     * @return the number of system updates performed by this query.
     */
    int getSystemUpdates();

    /**
     * If the query updated the graph in any way, this method will return true.
     *
     * @return if the graph has been updated.
     */
    boolean containsUpdates();

    /**
     * If the query updated the system graph in any way, this method will return true,
     *
     * @return if the system graph has been updated.
     */
    boolean containsSystemUpdates();

    QueryStatistics EMPTY = new QueryStatistics()
    {
        @Override
        public int getNodesCreated()
        {
            return 0;
        }

        @Override
        public int getNodesDeleted()
        {
            return 0;
        }

        @Override
        public int getRelationshipsCreated()
        {
            return 0;
        }

        @Override
        public int getRelationshipsDeleted()
        {
            return 0;
        }

        @Override
        public int getPropertiesSet()
        {
            return 0;
        }

        @Override
        public int getLabelsAdded()
        {
            return 0;
        }

        @Override
        public int getLabelsRemoved()
        {
            return 0;
        }

        @Override
        public int getIndexesAdded()
        {
            return 0;
        }

        @Override
        public int getIndexesRemoved()
        {
            return 0;
        }

        @Override
        public int getConstraintsAdded()
        {
            return 0;
        }

        @Override
        public int getConstraintsRemoved()
        {
            return 0;
        }

        @Override
        public int getSystemUpdates()
        {
            return 0;
        }

        @Override
        public boolean containsUpdates()
        {
            return false;
        }

        @Override
        public boolean containsSystemUpdates()
        {
            return false;
        }
    };
}
