/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

class TransactionMarkForTerminationFailedResult extends TransactionMarkForTerminationResult
{
    private static final String FAILURE_MESSAGE = "Transaction not found.";

    TransactionMarkForTerminationFailedResult( String transactionId, String userName )
    {
        super( transactionId, userName, FAILURE_MESSAGE );
    }

    TransactionMarkForTerminationFailedResult( String transactionId, String userName, String message )
    {
        super( transactionId, userName, message );
    }
}
