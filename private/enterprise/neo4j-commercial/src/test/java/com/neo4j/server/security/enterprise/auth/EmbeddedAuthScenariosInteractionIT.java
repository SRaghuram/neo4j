/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

@ExtendWith( EphemeralFileSystemExtension.class )
public class EmbeddedAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<CommercialLoginContext>
{
    @Inject
    private FileSystemAbstraction fileSystem;

    @Override
    protected NeoInteractionLevel<CommercialLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, () -> new UncloseableDelegatingFileSystemAbstraction( fileSystem ) );
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
