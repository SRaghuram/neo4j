/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

/**
 * Acceptor state for a Paxos instance
 */
public class AcceptorInstance
{
    private long ballot = -1;
    private Object value;

    public long getBallot()
    {
        return ballot;
    }

    public Object getValue()
    {
        return value;
    }

    public void promise( long ballot )
    {
        this.ballot = ballot;
    }

    public void accept( Object value )
    {
        this.value = value;
    }
}
