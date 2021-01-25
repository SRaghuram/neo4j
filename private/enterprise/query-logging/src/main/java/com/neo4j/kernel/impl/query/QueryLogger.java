/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import org.neo4j.kernel.api.query.ExecutingQuery;

public interface QueryLogger
{
    QueryLogger NO_LOG = new QueryLogger()
    {
        @Override
        public void start( ExecutingQuery query )
        {
        }

        @Override
        public void success( ExecutingQuery query )
        {
        }

        @Override
        public void failure( ExecutingQuery query, Throwable throwable )
        {
        }

        @Override
        public void failure( ExecutingQuery query, String reason )
        {
        }
    };

    void start( ExecutingQuery query );
    void success( ExecutingQuery query );
    void failure( ExecutingQuery query, Throwable throwable );
    void failure( ExecutingQuery query, String reason );
}
