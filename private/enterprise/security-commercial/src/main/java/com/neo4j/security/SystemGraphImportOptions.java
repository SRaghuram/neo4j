/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import java.util.function.Supplier;

import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.RoleRepository;

public class SystemGraphImportOptions
{
    public boolean shouldPerformImport;
    public boolean mayPerformMigration;
    public boolean shouldPurgeImportRepositoriesAfterSuccesfulImport;
    public boolean shouldResetSystemGraphAuthBeforeImport;
    public Supplier<UserRepository> importUserRepositorySupplier;
    public Supplier<RoleRepository> importRoleRepositorySupplier;
    public Supplier<UserRepository> migrationUserRepositorySupplier;
    public Supplier<RoleRepository> migrationRoleRepositorySupplier;
    public Supplier<UserRepository> initialUserRepositorySupplier;
    public Supplier<UserRepository> defaultAdminRepositorySupplier;

    public SystemGraphImportOptions(
            boolean shouldPerformImport,
            boolean mayPerformMigration,
            boolean shouldPurgeImportRepositoriesAfterSuccesfulImport,
            boolean shouldResetSystemGraphAuthBeforeImport,
            Supplier<UserRepository> importUserRepositorySupplier,
            Supplier<RoleRepository> importRoleRepositorySupplier,
            Supplier<UserRepository> migrationUserRepositorySupplier,
            Supplier<RoleRepository> migrationRoleRepositorySupplier,
            Supplier<UserRepository> initialUserRepositorySupplier,
            Supplier<UserRepository> defaultAdminRepositorySupplier
    )
    {
        this.shouldPerformImport = shouldPerformImport;
        this.mayPerformMigration = mayPerformMigration;
        this.shouldPurgeImportRepositoriesAfterSuccesfulImport = shouldPurgeImportRepositoriesAfterSuccesfulImport;
        this.shouldResetSystemGraphAuthBeforeImport = shouldResetSystemGraphAuthBeforeImport;
        this.importUserRepositorySupplier = importUserRepositorySupplier;
        this.importRoleRepositorySupplier = importRoleRepositorySupplier;
        this.migrationUserRepositorySupplier = migrationUserRepositorySupplier;
        this.migrationRoleRepositorySupplier = migrationRoleRepositorySupplier;
        this.initialUserRepositorySupplier = initialUserRepositorySupplier;
        this.defaultAdminRepositorySupplier = defaultAdminRepositorySupplier;
    }

}
