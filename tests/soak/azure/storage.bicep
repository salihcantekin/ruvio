// Long-lived storage for soak results: one storage account with a private `soak` container.
// Deploy once into a resource group that is not deleted after a run (cost: a few cents a
// month). Blobs older than 90 days are deleted.
//
// az group create -n rg-ruvio-soak-results -l uksouth
// az deployment group create -g rg-ruvio-soak-results -f tests/soak/azure/storage.bicep -p accountName=<globally unique>
//
// The controller writes with a container SAS that allows create and write only; readers get a
// separate read and list SAS. Both are made with `az storage container generate-sas`, kept
// outside the repository, and short-lived.

param location string = resourceGroup().location

@minLength(3)
@maxLength(24)
@description('Globally unique, lowercase letters and digits.')
param accountName string

param retentionDays int = 90

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: accountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  tags: { purpose: 'ruvio-soak' }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    // SAS tokens are signed with the account key.
    allowSharedKeyAccess: true
  }
}

resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: account
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'soak'
  properties: { publicAccess: 'None' }
}

resource lifecycle 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  parent: account
  name: 'default'
  properties: {
    policy: {
      rules: [
        {
          name: 'expire-old-runs'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: { blobTypes: ['blockBlob'], prefixMatch: ['soak/'] }
            actions: { baseBlob: { delete: { daysAfterModificationGreaterThan: retentionDays } } }
          }
        }
      ]
    }
  }
}

output containerUrl string = '${account.properties.primaryEndpoints.blob}soak'
