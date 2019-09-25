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
package com.neo4j.fabric.stream.summary;

import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.graphdb.QueryStatistics;

public class PartialSummary extends EmptySummary
{
    private final QueryStatistics queryStatistics;

    public PartialSummary( QueryStatistics queryStatistics )
    {
        this.queryStatistics = queryStatistics;
    }

    public PartialSummary( SummaryCounters counters )
    {
        this(new WrappingQueryStatistics( counters ));
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return queryStatistics;
    }

    static class WrappingQueryStatistics implements QueryStatistics
    {
        private final SummaryCounters counters;

        WrappingQueryStatistics(SummaryCounters counters)
        {
            this.counters = counters;
        }

        @Override
        public int getNodesCreated()
        {
            return counters.nodesCreated();
        }

        @Override
        public int getNodesDeleted()
        {
            return counters.nodesDeleted();
        }

        @Override
        public int getRelationshipsCreated()
        {
            return counters.relationshipsCreated();
        }

        @Override
        public int getRelationshipsDeleted()
        {
            return counters.relationshipsDeleted();
        }

        @Override
        public int getPropertiesSet()
        {
            return counters.propertiesSet();
        }

        @Override
        public int getLabelsAdded()
        {
            return counters.labelsAdded();
        }

        @Override
        public int getLabelsRemoved()
        {
            return counters.labelsRemoved();
        }

        @Override
        public int getIndexesAdded()
        {
            return counters.indexesAdded();
        }

        @Override
        public int getIndexesRemoved()
        {
            return counters.indexesRemoved();
        }

        @Override
        public int getConstraintsAdded()
        {
            return counters.constraintsAdded();
        }

        @Override
        public int getConstraintsRemoved()
        {
            return counters.constraintsRemoved();
        }

        @Override
        public int getSystemUpdates()
        {
            return 0;
        }

        @Override
        public boolean containsUpdates()
        {
            return counters.containsUpdates();
        }

        @Override
        public boolean containsSystemUpdates()
        {
            return false;
        }
    }
}
