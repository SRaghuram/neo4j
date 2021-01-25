/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

public enum RoleInfo
{
    //TODO: Factor out for the Role which already exists in cc.consensus.roles
    LEADER,
    FOLLOWER,
    READ_REPLICA,
    UNKNOWN
}
