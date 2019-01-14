/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

interface CausalClusteringStatus
{
    Response discover();

    Response available();

    Response readonly();

    Response writable();

    Response description();
}
