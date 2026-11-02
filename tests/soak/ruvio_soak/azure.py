"""Deleting the run's own resource group from the controller, so a long run does not depend on
anyone being awake at its end. The controller's managed identity (main.bicep, `selfDelete`)
is Contributor on its own group only.

Safety: the group is deleted only when the instance metadata names exactly the group given on
the command line, and only after the caller saw every result uploaded."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request

IMDS = "http://169.254.169.254/metadata"


def _imds(path: str, **params) -> dict:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(f"{IMDS}/{path}?{query}", headers={"Metadata": "true"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def delete_own_group(expected: str, log=print) -> bool:
    """Starts the deletion of the resource group this VM is in (asynchronous on Azure's side:
    the request returns at once and this VM is deleted with the rest)."""
    compute = _imds("instance/compute", **{"api-version": "2021-02-01"})
    group, subscription = compute["resourceGroupName"], compute["subscriptionId"]
    if group.lower() != expected.lower():
        log(f"not deleting: this VM is in {group}, not {expected}")
        return False
    token = _imds("identity/oauth2/token", **{"api-version": "2018-02-01", "resource": "https://management.azure.com/"})
    url = (f"https://management.azure.com/subscriptions/{subscription}/resourcegroups/"
           f"{urllib.parse.quote(group)}?api-version=2021-04-01")
    request = urllib.request.Request(url, method="DELETE", headers={"Authorization": f"Bearer {token['access_token']}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        log(f"resource group {group}: delete requested ({response.status})")
        return 200 <= response.status < 300


def deallocate_all(expected: str, log=print) -> int:
    """The backstop of a run that did not end on its own: deallocates every VM of this
    resource group (the controller last), keeping the disks for a look afterwards. Returns the
    number of VMs asked to stop."""
    compute = _imds("instance/compute", **{"api-version": "2021-02-01"})
    group, subscription, me = compute["resourceGroupName"], compute["subscriptionId"], compute["name"]
    if group.lower() != expected.lower():
        log(f"not stopping anything: this VM is in {group}, not {expected}")
        return 0
    token = _imds("identity/oauth2/token", **{"api-version": "2018-02-01", "resource": "https://management.azure.com/"})
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    base = (f"https://management.azure.com/subscriptions/{subscription}/resourceGroups/{urllib.parse.quote(group)}"
            "/providers/Microsoft.Compute/virtualMachines")
    with urllib.request.urlopen(urllib.request.Request(f"{base}?api-version=2024-07-01", headers=headers), timeout=30) as response:
        names = [vm["name"] for vm in json.loads(response.read())["value"]]
    names.sort(key=lambda name: name == me)  # this VM last: the request outlives it
    for name in names:
        request = urllib.request.Request(f"{base}/{urllib.parse.quote(name)}/deallocate?api-version=2024-07-01",
                                         method="POST", data=b"", headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            log(f"{name}: deallocate requested ({response.status})")
    return len(names)
