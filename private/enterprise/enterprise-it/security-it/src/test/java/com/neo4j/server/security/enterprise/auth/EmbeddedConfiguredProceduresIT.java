/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.junit.jupiter.api.TestInfo;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;

public class EmbeddedConfiguredProceduresIT extends ConfiguredProceduresTestBase<EnterpriseLoginContext>
{

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<Setting<?>,String> config, TestInfo testInfo )
    {
        return new EmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }
}
