/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.string.UTF8;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

@Command(
        name = "set-operator-password",
        description = "Sets the password of the loopback operator user. ",
        hidden = true
)
public class SetOperatorPasswordCommand extends AbstractCommand
{
    @Parameters
    private String password;

    public SetOperatorPasswordCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute() throws IOException
    {
        Config config = loadNeo4jConfig();
        FileSystemAbstraction fileSystem = ctx.fs();

        Path file = EnterpriseSecurityModule.getOperatorUserRepositoryFile( config );
        if ( fileSystem.fileExists( file ) )
        {
            fileSystem.deleteFile( file );
        }

        FileUserRepository userRepository =
                new FileUserRepository( fileSystem, file, NullLogProvider.getInstance() );
        try
        {
            userRepository.start();
            userRepository.create(
                    new User.Builder( "LOOPBACK", SystemGraphCredential.createCredentialForPassword( UTF8.encode( password ), new SecureHasher() ) )
                            .withRequiredPasswordChange( false )
                            .build()
            );
            userRepository.shutdown();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        ctx.out().println( "Changed password for operator user." );
    }

    Config loadNeo4jConfig()
    {
        Config cfg = Config.newBuilder()
                           .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath() )
                           .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                           .commandExpansion( allowCommandExpansion )
                           .build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }
}
