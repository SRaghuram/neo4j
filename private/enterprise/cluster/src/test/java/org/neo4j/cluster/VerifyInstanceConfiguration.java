/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VerifyInstanceConfiguration
{
    public final List<URI> members;
    public final Map<String, InstanceId> roles;
    public final Set<InstanceId> failed;

    public VerifyInstanceConfiguration( List<URI> members, Map<String, InstanceId> roles, Set<InstanceId> failed )
    {
        this.members = members;
        this.roles = roles;
        this.failed = failed;
    }
}
