/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal.util;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;

class ImportOptionsBuilder
{
    private boolean shouldPerformImport;
    private boolean mayPerformMigration;
    private boolean shouldPurgeImportRepositoriesAfterSuccesfulImport;
    private boolean shouldResetSystemGraphAuthBeforeImport;
    private String[] importUsers = new String[0];
    private List<Pair<String,String[]>> importRoles = new ArrayList<>();
    private String[] migrateUsers = new String[0];
    private List<Pair<String,String[]>> migrateRoles = new ArrayList<>();
    private String[] initialUsers = new String[0];
    private String[] defaultAdmins = new String[0];

    ImportOptionsBuilder()
    {
    }

    ImportOptionsBuilder shouldPerformImport()
    {
        shouldPerformImport = true;
        return this;
    }

    ImportOptionsBuilder shouldNotPerformImport()
    {
        shouldPerformImport = false;
        return this;
    }

    ImportOptionsBuilder mayPerformMigration()
    {
        mayPerformMigration = true;
        return this;
    }

    ImportOptionsBuilder mayNotPerformMigration()
    {
        mayPerformMigration = false;
        return this;
    }

    ImportOptionsBuilder shouldPurgeImportRepositoriesAfterSuccesfulImport()
    {
        shouldPurgeImportRepositoriesAfterSuccesfulImport = true;
        return this;
    }

    ImportOptionsBuilder shouldNotPurgeImportRepositoriesAfterSuccesfulImport()
    {
        shouldPurgeImportRepositoriesAfterSuccesfulImport = false;
        return this;
    }

    ImportOptionsBuilder shouldResetSystemGraphAuthBeforeImport()
    {
        shouldResetSystemGraphAuthBeforeImport = true;
        return this;
    }

    ImportOptionsBuilder shouldNotResetSystemGraphAuthBeforeImport()
    {
        shouldResetSystemGraphAuthBeforeImport = false;
        return this;
    }

    ImportOptionsBuilder importUsers( String... importUsers )
    {
        this.importUsers = importUsers;
        return this;
    }

    ImportOptionsBuilder importRole( String role, String... users )
    {
        this.importRoles.add( Pair.of( role, users ) );
        return this;
    }

    ImportOptionsBuilder migrateUsers( String... migrateUsers )
    {
        this.migrateUsers = migrateUsers;
        return this;
    }

    ImportOptionsBuilder migrateRole( String role, String... users )
    {
        this.migrateRoles.add( Pair.of( role, users ) );
        return this;
    }

    ImportOptionsBuilder initialUsers( String... initialUsers )
    {
        this.initialUsers = initialUsers;
        return this;
    }

    ImportOptionsBuilder defaultAdmins( String... defaultAdmins )
    {
        this.defaultAdmins = defaultAdmins;
        return this;
    }

    @SuppressWarnings( "unchecked" )
    SystemGraphImportOptions build() throws IOException, InvalidArgumentsException
    {
        return testImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUsers,
                importRoles.toArray( new Pair[0] ),
                migrateUsers,
                migrateRoles.toArray( new Pair[0] ),
                initialUsers,
                defaultAdmins );
    }

    private static SystemGraphImportOptions testImportOptions(
            boolean shouldPerformImport,
            boolean mayPerformMigration,
            boolean shouldPurgeImportRepositoriesAfterSuccesfulImport,
            boolean shouldResetSystemGraphAuthBeforeImport,
            String[] importUsers,
            Pair<String,String[]>[] importRoles,
            String[] migrateUsers,
            Pair<String,String[]>[] migrateRoles,
            String[] initialUsers,
            String[] defaultAdmins
    ) throws IOException, InvalidArgumentsException
    {
        UserRepository importUserRepository = new InMemoryUserRepository();
        RoleRepository importRoleRepository = new InMemoryRoleRepository();
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        RoleRepository migrationRoleRepository = new InMemoryRoleRepository();
        UserRepository initialUserRepository = new InMemoryUserRepository();
        UserRepository defaultAdminRepository = new InMemoryUserRepository();

        populateUserRepository( importUserRepository, importUsers );
        populateRoleRepository( importRoleRepository, importRoles );
        populateUserRepository( migrationUserRepository, migrateUsers );
        populateRoleRepository( migrationRoleRepository, migrateRoles );
        populateUserRepository( initialUserRepository, initialUsers );
        populateUserRepository( defaultAdminRepository, defaultAdmins );

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                () -> importUserRepository,
                () -> importRoleRepository,
                () -> migrationUserRepository,
                () -> migrationRoleRepository,
                () -> initialUserRepository,
                () -> defaultAdminRepository
        );
    }

    private static void populateUserRepository( UserRepository repository, String[] usernames ) throws IOException, InvalidArgumentsException
    {
        for ( String username : usernames )
        {
            // Use username as password to simplify test assertions
            User user = new User.Builder( username, LegacyCredential.forPassword( username ) ).build();
            repository.create( user );
        }
    }

    private static void populateRoleRepository( RoleRepository repository, Pair<String,String[]>[] roles ) throws IOException, InvalidArgumentsException
    {
        for ( Pair<String,String[]> role : roles )
        {
            RoleRecord roleRecord = new RoleRecord( role.first(), role.other() );
            repository.create( roleRecord );
        }
    }
}
