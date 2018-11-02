/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import java.util.Map;

import org.neo4j.kernel.impl.util.ValueUtils;

public class BoltConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<BoltInteraction.BoltSubject>
{
    public BoltConfiguredAuthScenariosInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer( Map<String, String> config )
    {
        return new BoltInteraction( config );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return ValueUtils.of( obj );
    }

}
