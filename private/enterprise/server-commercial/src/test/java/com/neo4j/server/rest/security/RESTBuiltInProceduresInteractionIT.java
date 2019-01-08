/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.neo4j.server.security.enterprise.auth.BuiltInProceduresInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

@ExtendWith( SuppressOutputExtension.class )
public class RESTBuiltInProceduresInteractionIT extends BuiltInProceduresInteractionTestBase<RESTSubject>
{
    @Inject
    SuppressOutput suppressOutput;

    public RESTBuiltInProceduresInteractionIT()
    {
        super();
        CHANGE_PWD_ERR_MSG = "User is required to change their password.";
        PWD_CHANGE_CHECK_FIRST = true;
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<RESTSubject> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new RESTInteraction( config );
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
