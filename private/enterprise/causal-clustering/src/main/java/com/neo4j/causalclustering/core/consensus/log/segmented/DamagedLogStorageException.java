/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

// TODO: Should this really be a KernelException?
public class DamagedLogStorageException extends KernelException
{
    public DamagedLogStorageException( String format, Object... args )
    {
        super( Status.General.StorageDamageDetected, format, args );
    }

    public DamagedLogStorageException( Throwable cause, String format, Object... args )
    {
        super( Status.General.StorageDamageDetected, cause, format, args );
    }
}
