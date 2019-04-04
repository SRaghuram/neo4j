/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.SystemGraphCredential;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

class BasicImportOptionsBuilder
{
    private String[] migrateUsers = new String[0];
    private String[] initialUsers = new String[0];

    private String migratePassword;
    private String initialPassword;

    private boolean migratePasswordChangeRequired;
    private boolean initialPasswordChangeRequired;

    BasicImportOptionsBuilder()
    {
    }

    BasicImportOptionsBuilder migrateUser( String userName, String password, boolean pwdChangeRequired )
    {
        this.migrateUsers = new String[]{ userName };
        this.migratePassword = password;
        this.migratePasswordChangeRequired = pwdChangeRequired;
        return this;
    }

    BasicImportOptionsBuilder migrateUsers( String... migrateUsers )
    {
        this.migrateUsers = migrateUsers;
        return this;
    }

    BasicImportOptionsBuilder initialUser( String password, boolean pwdChangeRequired )
    {
        this.initialUsers = new String[]{ INITIAL_USER_NAME };
        this.initialPassword = password;
        this.initialPasswordChangeRequired = pwdChangeRequired;
        return this;
    }

    Supplier<UserRepository> migrationSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        populateUserRepository( migrationUserRepository, migrateUsers, migratePassword, migratePasswordChangeRequired );
        return () -> migrationUserRepository;
    }

    Supplier<UserRepository> initalUserSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        populateUserRepository( migrationUserRepository, initialUsers, initialPassword, initialPasswordChangeRequired );
        return () -> migrationUserRepository;
    }

    private static void populateUserRepository( UserRepository repository, String[] usernames, String password, boolean pwdChangeRequired )
            throws IOException, InvalidArgumentsException
    {
        for ( String username : usernames )
        {
            byte[] encodedPassword;

            if ( password != null )
            {
                encodedPassword = UTF8.encode( password );
            }
            else
            {
                // Use username as password to simplify test assertions
                encodedPassword = UTF8.encode( username );
            }

            SystemGraphCredential credential = SystemGraphCredential.createCredentialForPassword( encodedPassword, new SecureHasher() );

            User user = new User.Builder( username, credential ).withRequiredPasswordChange( pwdChangeRequired ).build();
            repository.create( user );
        }
    }
}
