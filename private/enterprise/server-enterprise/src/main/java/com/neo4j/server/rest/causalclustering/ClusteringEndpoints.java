/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

interface ClusteringEndpoints
{
    Response discover();

    Response available();

    Response readonly();

    Response writable();

    Response description();
}
