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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CompilerLibrary;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.logging.LogProvider;

/**
 * To run a Cypher query, use this class.
 *
 * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
 * operation so please make sure this will be constructed only once and properly reused.
 *
 */
public class SystemExecutionEngine extends ExecutionEngine
{
    private org.neo4j.cypher.internal.ExecutionEngine normalExecutionEngine;

    /**
     * Creates an execution engine around the given graph database wrapping an internal compiler factory for two level Cypher runtime.
     * This is used for processing system database commands, where the outer Cypher engine will only understand DDL commands
     * and translate them into normal Cypher against the SYSTEM database, processed by the inner Cypher runtime, which understands normal Cypher.
     */
    public SystemExecutionEngine( GraphDatabaseQueryService queryService, LogProvider logProvider, CompilerFactory systemCompilerFactory, CompilerFactory normalCompilerFactory )
    {
        normalExecutionEngine = makeExecutionEngine(queryService, logProvider, new CompilerLibrary(normalCompilerFactory, this::normalExecutionEngine));
        cypherExecutionEngine = makeExecutionEngine(queryService, logProvider, new CompilerLibrary(systemCompilerFactory, this::normalExecutionEngine));
    }

    private org.neo4j.cypher.internal.ExecutionEngine normalExecutionEngine() {
        return normalExecutionEngine;
    }
}
