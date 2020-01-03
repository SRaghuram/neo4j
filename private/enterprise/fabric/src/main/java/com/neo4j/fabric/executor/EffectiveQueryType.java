/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.planning.QueryType;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.graphdb.QueryExecutionType;

public class EffectiveQueryType
{
    public static QueryExecutionType.QueryType effectiveQueryType( AccessMode requested, QueryType queryType )
    {
        if ( queryType == QueryType.READ() )
        {
            return QueryExecutionType.QueryType.READ_ONLY;
        }
        if ( queryType == QueryType.READ_PLUS_UNRESOLVED() )
        {
            switch ( requested )
            {
            case READ:
                return QueryExecutionType.QueryType.READ_ONLY;
            case WRITE:
                return QueryExecutionType.QueryType.READ_WRITE;
            default:
                throw new IllegalArgumentException( "Unexpected access mode: " + requested );
            }
        }
        if ( queryType == QueryType.WRITE() )
        {
            return QueryExecutionType.QueryType.READ_WRITE;
        }

        throw new IllegalArgumentException( "Unexpected query type: " + queryType );
    }

    public static AccessMode effectiveAccessMode( AccessMode requested, QueryType queryType )
    {
        if ( queryType == QueryType.READ() )
        {
            return AccessMode.READ;
        }
        if ( queryType == QueryType.READ_PLUS_UNRESOLVED() )
        {
            return requested;
        }
        if ( queryType == QueryType.WRITE() )
        {
            return AccessMode.WRITE;
        }

        throw new IllegalArgumentException( "Unexpected query type: " + queryType );
    }
}
