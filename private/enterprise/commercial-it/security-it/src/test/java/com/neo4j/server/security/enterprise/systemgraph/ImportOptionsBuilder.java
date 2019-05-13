/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;

public class ImportOptionsBuilder extends BasicImportOptionsBuilder
{
    private boolean shouldPerformImport;
    private boolean mayPerformMigration;
    private boolean shouldPurgeImportRepositoriesAfterSuccesfulImport;
    private boolean shouldResetSystemGraphAuthBeforeImport;
    private List<User> importUsers = new ArrayList<>();
    private List<Pair<String,String[]>> importRoles = new ArrayList<>();
    private List<Pair<String,String[]>> migrateRoles = new ArrayList<>();
    private List<User> defaultAdmins = new ArrayList<>();

    public ImportOptionsBuilder()
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
        return (ImportOptionsBuilder) fillListWithUsers( this.importUsers, importUsers );
    }

    ImportOptionsBuilder importRole( String role, String... users )
    {
        this.importRoles.add( Pair.of( role, users ) );
        return this;
    }

    ImportOptionsBuilder migrateUser( String userName, String password, boolean pwdChangeRequired )
    {
        return (ImportOptionsBuilder) super.migrateUser( userName, password, pwdChangeRequired );
    }

    ImportOptionsBuilder migrateUsers( String... migrateUsers )
    {
        return (ImportOptionsBuilder) super.migrateUsers( migrateUsers );
    }

    ImportOptionsBuilder migrateRole( String role, String... users )
    {
        this.migrateRoles.add( Pair.of( role, users ) );
        return this;
    }

    ImportOptionsBuilder initialUser( String password, boolean pwdChangeRequired )
    {
        return (ImportOptionsBuilder) super.initialUser( password, pwdChangeRequired );
    }

    ImportOptionsBuilder initialUsers( String... initialUsers )
    {
        return (ImportOptionsBuilder) fillListWithUsers( this.initialUsers, initialUsers );
    }

    ImportOptionsBuilder defaultAdmins( String... defaultAdmins )
    {
        return (ImportOptionsBuilder) fillListWithUsers( this.defaultAdmins, defaultAdmins );
    }

    @SuppressWarnings( "unchecked" )
    public SystemGraphImportOptions build() throws IOException, InvalidArgumentsException
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
            List<User> importUsers,
            Pair<String,String[]>[] importRoles,
            List<User> migrateUsers,
            Pair<String,String[]>[] migrateRoles,
            List<User> initialUsers,
            List<User> defaultAdmins
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

    private static void populateRoleRepository( RoleRepository repository, Pair<String,String[]>[] roles ) throws IOException, InvalidArgumentsException
    {
        for ( Pair<String,String[]> role : roles )
        {
            RoleRecord roleRecord = new RoleRecord( role.first(), role.other() );
            repository.create( roleRecord );
        }
    }
}
