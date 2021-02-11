#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

Import-Module "$PSScriptRoot\Neo4j-Management.psd1"
$Arguments = Get-Args $args
Exit (Invoke-Neo4j -Verbose:$Arguments.Verbose -CommandArgs $Arguments.Args)

