/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public interface TransactionRepresentationExtractor
{
    TransactionRepresentation extract( TransactionRepresentationReplicatedTransaction replicatedTransaction );

    TransactionRepresentation extract( ByteArrayReplicatedTransaction replicatedTransaction );
}
