/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.neo4j.server.security.enterprise.auth.AuthScenariosInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
public class RESTAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<RESTSubject>
{
    @Inject
    SuppressOutput suppressOutput;

    RESTAuthScenariosInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
    }

    @Override
    protected NeoInteractionLevel<RESTSubject> setUpNeoServer( Map<Setting<?>,String> config ) throws Throwable
    {
        return new RESTInteraction( config, testDirectory.homeDir() );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Long )
        {
            return ((Long) obj).intValue();
        }
        else
        {
            return obj;
        }
    }
}
