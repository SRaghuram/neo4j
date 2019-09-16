/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.util.ValueUtils;

public class BoltUserManagementProceduresInteractionIT extends AuthProceduresInteractionTestBase<BoltInteraction.BoltSubject>
{

    public BoltUserManagementProceduresInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer( Map<Setting<?>, String> config )
    {
        return new BoltInteraction( config );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return ValueUtils.of( obj );
    }
}
