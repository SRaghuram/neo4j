/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

public interface RemoteControl
{
    void stop();

    /**
     * @param pause null for toggle
     */
    void pause( Boolean pause );

    /**
     * @param pause null for toggle
     */
    void pauseKiller( Boolean pause );

    void addListener( RemoteControlListener listener );

    void removeListener( RemoteControlListener listener );
}
