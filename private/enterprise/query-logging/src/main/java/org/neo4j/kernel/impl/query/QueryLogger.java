/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.query;

import org.neo4j.kernel.api.query.ExecutingQuery;

public interface QueryLogger
{
    QueryLogger NO_LOG = new QueryLogger()
    {
        @Override
        public void success( ExecutingQuery query )
        {
        }

        @Override
        public void failure( ExecutingQuery query, Throwable throwable )
        {
        }
    };

    void success( ExecutingQuery query );
    void failure( ExecutingQuery query, Throwable throwable );
}
