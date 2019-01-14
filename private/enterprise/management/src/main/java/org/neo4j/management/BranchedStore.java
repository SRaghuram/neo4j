/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@Deprecated
@ManagementInterface( name = BranchedStore.NAME )
@Description( "Information about the branched stores present in this HA cluster member" )
public interface BranchedStore
{
    String NAME = "Branched Store";

    @Description( "A list of the branched stores" )
    BranchedStoreInfo[] getBranchedStores();
}
