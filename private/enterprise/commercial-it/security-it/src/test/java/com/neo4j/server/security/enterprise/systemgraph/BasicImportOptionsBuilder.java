/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

class BasicImportOptionsBuilder
{
    List<User> migrateUsers = new ArrayList<>();
    List<User> initialUsers = new ArrayList<>();

    BasicImportOptionsBuilder()
    {
    }

    BasicImportOptionsBuilder migrateUser( String userName, String password, boolean pwdChangeRequired )
    {
        migrateUsers.add( createUser( userName, password, pwdChangeRequired ) );
        return this;
    }

    BasicImportOptionsBuilder migrateUsers( String... migrateUsers )
    {
        return fillListWithUsers( this.migrateUsers, migrateUsers );
    }

    BasicImportOptionsBuilder initialUser( String password, boolean pwdChangeRequired )
    {
        this.initialUsers.add( createUser( INITIAL_USER_NAME, password, pwdChangeRequired ) );
        return this;
    }

    Supplier<UserRepository> migrationSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        populateUserRepository( migrationUserRepository, migrateUsers );
        return () -> migrationUserRepository;
    }

    Supplier<UserRepository> initalUserSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository initialUserRepository = new InMemoryUserRepository();
        populateUserRepository( initialUserRepository, initialUsers );
        return () -> initialUserRepository;
    }

    static void populateUserRepository( UserRepository repository, List<User> users ) throws IOException, InvalidArgumentsException
    {
        for ( User user : users )
        {
            repository.create( user );
        }
    }

    BasicImportOptionsBuilder fillListWithUsers( List<User> list, String... userNames )
    {
        for ( String userName :userNames )
        {
            // Use username as password to simplify test assertions
            list.add( createUser( userName, userName, false ) );
        }
        return this;
    }

    private static User createUser( String userName, String password, boolean pwdChangeRequired )
    {
        return new User.Builder( userName, credentialFor( password ) ).withRequiredPasswordChange( pwdChangeRequired ).build();
    }
}
