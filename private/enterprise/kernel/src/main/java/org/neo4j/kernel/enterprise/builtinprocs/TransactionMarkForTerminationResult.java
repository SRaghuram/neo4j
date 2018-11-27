/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

public class TransactionMarkForTerminationResult
{
    private static final String TERMINATION_MESSAGE = "Transaction terminated.";

    public final String transactionId;
    public final String username;
    public final String message;

    TransactionMarkForTerminationResult( String transactionId, String userName )
    {
        this( transactionId, userName, TERMINATION_MESSAGE );
    }

    TransactionMarkForTerminationResult( String transactionId, String username, String message )
    {
        this.transactionId = transactionId;
        this.username = username;
        this.message = message;
    }
}
