// A monthly cost alert for the soak resource groups. The only email the soak setup sends.
//
// az deployment sub create -l uksouth -f tests/soak/azure/budget.bicep \
//   -p amount=50 contactEmail=<address> startDate=2026-11-01

targetScope = 'subscription'

@description('Monthly amount in the billing currency.')
param amount int = 50

param contactEmail string

@description('First day of a month, yyyy-MM-dd; not in the past.')
param startDate string

@description('Resource groups the budget counts; the run group is deleted and created again under the same name.')
param resourceGroups array = ['rg-ruvio-soak', 'rg-ruvio-soak-results']

resource budget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: 'ruvio-soak-monthly'
  properties: {
    category: 'Cost'
    amount: amount
    timeGrain: 'Monthly'
    timePeriod: { startDate: startDate }
    filter: {
      dimensions: { name: 'ResourceGroupName', operator: 'In', values: resourceGroups }
    }
    notifications: {
      actual80: {
        enabled: true
        operator: 'GreaterThanOrEqualTo'
        threshold: 80
        thresholdType: 'Actual'
        contactEmails: [contactEmail]
      }
      forecast100: {
        enabled: true
        operator: 'GreaterThanOrEqualTo'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: [contactEmail]
      }
    }
  }
}
