/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

/**
 * Represents a component that differs between master, slave and pending HA instance states and can be switched to
 * either of three.
 */
public interface ComponentSwitcher
{
    void switchToMaster();

    void switchToSlave();

    void switchToPending();
}
