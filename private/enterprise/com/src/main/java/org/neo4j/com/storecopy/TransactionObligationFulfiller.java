/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

/**
 * Fulfills transaction obligations, i.e. ensures that the database has committed and applied a particular
 * transaction id.
 */
public interface TransactionObligationFulfiller
{
    void fulfill( long toTxId ) throws InterruptedException;
}
