/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import org.neo4j.annotations.service.ServiceProvider;

@Deprecated
@ServiceProvider
public class HotspotManagementSupport extends AdvancedManagementSupport
{
    // On HotSpot, all the default behaviour just works.
}
