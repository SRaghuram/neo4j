/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.store;

public class UnableToCopyStoreFromOldMasterException extends HighAvailabilityStoreFailureException
{
    private static final String MESSAGE_PATTERN = "Can't copy store from old master. " +
            "My protocol version is %s, master's protocol version is %s";

    public UnableToCopyStoreFromOldMasterException( byte myProtocolVersion, byte masterProtocolVersion )
    {
        super( String.format( MESSAGE_PATTERN, myProtocolVersion, masterProtocolVersion ) );
    }
}
