/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public interface TransactionStream
{
    void accept( Visitor<CommittedTransactionRepresentation,Exception> visitor ) throws Exception;

    TransactionStream EMPTY = visitor ->
    {
    };
}
