/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.neo4j.server.security.enterprise.auth.AuthProceduresInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.SuppressOutputExtension;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
public class RESTUserManagementProceduresInteractionIT extends AuthProceduresInteractionTestBase<RESTSubject>
{

    RESTUserManagementProceduresInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<RESTSubject> setUpNeoServer( Map<Setting<?>,String> config, TestInfo testInfo ) throws Throwable
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
