# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#
# Module manifest for module 'Neo4j-Management'
#


@{
ModuleVersion = '3.0.0'

GUID = '2a3e34b4-5564-488e-aaf6-f2cba3f7f05d'

Author = 'Neo4j'

CompanyName = 'Neo4j'

Copyright = 'https://neo4j.com/licensing/'

Description = 'Powershell module to manage a Neo4j instance on Windows'

PowerShellVersion = '2.0'

NestedModules = @('Neo4j-Management\Neo4j-Management.psm1')

FunctionsToExport = @(
'Invoke-Neo4j',
'Invoke-Neo4jAdmin',
'Get-Args'
)

CmdletsToExport = ''

VariablesToExport = ''

AliasesToExport = ''
}
