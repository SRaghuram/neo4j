/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public final class DbmsTransactionId extends DbmsId
{
    public static final String TRANSACTION_ID_SEPARATOR = "-transaction-";

    DbmsTransactionId( String database, long kernelTransactionId ) throws InvalidArgumentsException
    {
        super( database, kernelTransactionId, TRANSACTION_ID_SEPARATOR );
    }

    DbmsTransactionId( String external ) throws InvalidArgumentsException
    {
        super( external, TRANSACTION_ID_SEPARATOR );
    }
}
