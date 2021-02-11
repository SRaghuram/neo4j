#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

<#
.SYNOPSIS
Processes passed in arguments array and returns filtered fields for use in other functions

.DESCRIPTION
Filters passed-in arguments array and returns whether verbose option is enabled and the rest of the arguments

.PARAMETER Arguments
An array of arguments to process

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
function Get-Args
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromPipeline = $true)]
    [array]$Arguments
  )

  begin
  {
  }

  process
  {
    if (!$Arguments) {
      $Arguments = @()
    }

    $ActualArgs = $Arguments -notmatch "^-v$|^-verbose$"
    $Verbose = $ActualArgs.Count -lt $Arguments.Count
    $ArgsAsStr = $ActualArgs -join ' '

    Write-Output @{ 'Verbose' = $Verbose; 'Args' = $ActualArgs; 'ArgsAsStr' = $ArgsAsStr }
  }

  end
  {
  }
}
