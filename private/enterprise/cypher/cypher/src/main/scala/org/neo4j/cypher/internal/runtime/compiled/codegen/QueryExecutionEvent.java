/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen;

public interface QueryExecutionEvent extends AutoCloseable
{
    void dbHit();

    void row();

    @Override
    void close();

    QueryExecutionEvent NONE = new QueryExecutionEvent()
    {
        @Override
        public void dbHit()
        {
        }

        @Override
        public void row()
        {
        }

        @Override
        public void close()
        {
        }
    };
}
