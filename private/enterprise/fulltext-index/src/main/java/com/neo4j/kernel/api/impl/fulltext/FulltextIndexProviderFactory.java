/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.kernel.api.impl.fulltext;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class FulltextIndexProviderFactory extends KernelExtensionFactory<FulltextIndexProviderFactory.Dependencies>
{
    private static final String KEY = "fulltext";
    private static final int PRIORITY = 0;
    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );

    public FulltextIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, KEY );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( DESCRIPTOR );
        return directoriesBySubProvider( parentDirectoryStructure );
    }

    @Override
    public FulltextIndexProvider newInstance( KernelContext context, Dependencies dependencies )
    {
        Config config = dependencies.getConfig();
        boolean ephemeral = config.get( GraphDatabaseSettings.ephemeral );
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystem();
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, fileSystemAbstraction );
        OperationalMode operationalMode = context.databaseInfo().operationalMode;

        FulltextIndexProvider provider =
                new FulltextIndexProvider( DESCRIPTOR, PRIORITY, subProviderDirectoryStructure( context.storeDir() ), fileSystemAbstraction, config,
                        dependencies.tokenHolders(), directoryFactory, operationalMode );
        dependencies.procedures().registerComponent( FulltextAdapter.class, procContext -> provider, true );

        return provider;
    }

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction fileSystem();

        TokenHolders tokenHolders();

        Procedures procedures();
    }
}
