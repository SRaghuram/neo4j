/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.neo4j.kernel.impl.locking.ActiveLock;

public class ActiveLocksResult
{
    public final String mode;
    public final String resourceType;
    public final long resourceId;

    public ActiveLocksResult( ActiveLock lock )
    {
        this.mode = lock.mode();
        this.resourceType = lock.resourceType().name();
        this.resourceId = lock.resourceId();
    }
}
