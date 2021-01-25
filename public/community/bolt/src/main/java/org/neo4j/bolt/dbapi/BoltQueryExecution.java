/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.dbapi;

import org.neo4j.kernel.impl.query.QueryExecution;

/**
 * A representation of a query execution result.
 */
public interface BoltQueryExecution
{
    QueryExecution getQueryExecution();

    /**
     * This should be called once the query is finished, either successfully or not.
     * Should be called from the same thread the query was executing in.
     */
    void close();

    /**
     * This is used to terminate a currently running query. Can be called from any thread.
     */
    void terminate();
}
