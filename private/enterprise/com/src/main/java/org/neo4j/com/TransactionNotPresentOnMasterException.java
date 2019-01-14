/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

/**
 * Used to indicate that the master tried to resume a transaction that was not present
 */
public class TransactionNotPresentOnMasterException extends IllegalStateException
{
    public TransactionNotPresentOnMasterException( RequestContext txId )
    {
        super( "Transaction " + txId + " not present on master" );
    }
}
