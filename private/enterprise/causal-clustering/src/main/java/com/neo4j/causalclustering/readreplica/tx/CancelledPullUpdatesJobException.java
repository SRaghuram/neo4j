/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

public class CancelledPullUpdatesJobException extends Exception
{
    public static CancelledPullUpdatesJobException INSTANCE = new CancelledPullUpdatesJobException();

    private CancelledPullUpdatesJobException()
    {
    }
}
