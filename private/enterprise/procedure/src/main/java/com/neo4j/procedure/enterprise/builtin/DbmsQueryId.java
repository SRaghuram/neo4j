/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public final class DbmsQueryId extends DbmsId
{
    public static final String QUERY_ID_SEPARATOR = "-query-";

    DbmsQueryId( String database, long kernelQueryId ) throws InvalidArgumentsException
    {
        super( database, kernelQueryId, QUERY_ID_SEPARATOR );
    }

    DbmsQueryId( String external ) throws InvalidArgumentsException
    {
        super( external, QUERY_ID_SEPARATOR );
    }
}
