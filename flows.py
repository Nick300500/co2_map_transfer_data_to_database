"""Prefect flow entrypoints; thin wrappers so the transfer_*.py scripts stay standalone-runnable."""
from prefect import flow

import transfer_capacities
import transfer_demand_reg_factors
import transfer_mastr


@flow(name="transfer-mastr")
def transfer_mastr_flow():
    transfer_mastr.main()


@flow(name="transfer-capacities")
def transfer_capacities_flow():
    transfer_capacities.main()


@flow(name="transfer-demand-reg-factors")
def transfer_demand_reg_factors_flow():
    transfer_demand_reg_factors.main()
