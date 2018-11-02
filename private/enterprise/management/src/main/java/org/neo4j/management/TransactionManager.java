/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = TransactionManager.NAME )
@Description( "Information about the Neo4j transaction manager" )
public interface TransactionManager
{
    String NAME = "Transactions";

    @Description( "The number of currently open transactions" )
    long getNumberOfOpenTransactions();

    @Description( "The highest number of transactions ever opened concurrently" )
    long getPeakNumberOfConcurrentTransactions();

    @Description( "The total number started transactions" )
    long getNumberOfOpenedTransactions();

    @Description( "The total number of committed transactions" )
    long getNumberOfCommittedTransactions();

    @Description( "The total number of rolled back transactions" )
    long getNumberOfRolledBackTransactions();

    @Description( "The id of the latest committed transaction" )
    long getLastCommittedTxId();
}
