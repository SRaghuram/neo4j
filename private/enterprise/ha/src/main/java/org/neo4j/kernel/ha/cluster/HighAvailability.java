/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

/**
 * A place to register {@link HighAvailabilityMemberListener listeners}
 * that will receive events about (high) availability and roles in a cluster.
 *
 * @author Mattias Persson
 */
public interface HighAvailability
{
    void addHighAvailabilityMemberListener( HighAvailabilityMemberListener listener );

    void removeHighAvailabilityMemberListener( HighAvailabilityMemberListener listener );
}
