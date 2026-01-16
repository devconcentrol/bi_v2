from utils.logger import Logger
import pandas as pd

from sqlalchemy import (
    Engine,
)


class DimensionLookup:
    _con_dw: Engine
    _customer_map: dict | None
    _material_map: dict | None
    _agent_map: dict | None

    def __init__(self, con_dw: Engine):
        self._con_dw: Engine = con_dw
        self._customer_map = None
        self._material_map = None
        self._agent_map = None

    def invalidate_caches(self) -> None:
        self._customer_map = None
        self._material_map = None
        self._agent_map = None

    def _load_customers(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading customers from DB")
        customer_query = """SELECT CustId, CustCode, SalesOrganization, Channel, Division FROM CustomerDim"""
        return pd.read_sql(customer_query, self._con_dw)

    def _load_materials(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading materials from DB")
        material_query = "SELECT MaterialCode, MaterialId FROM MaterialsDim"
        return pd.read_sql(material_query, self._con_dw)

    def _load_agents(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading agents from DB")
        agent_query = "SELECT AgentId, AgentType, AgentCode FROM AgentDim"
        return pd.read_sql(agent_query, self._con_dw)

    def get_customer_map(self) -> dict:
        if self._customer_map is not None:
            return self._customer_map

        df_customer: pd.DataFrame = self._load_customers()

        # Cache the map for reuse
        self._customer_map = dict(
            zip(
                (
                    df_customer.SalesOrganization
                    + df_customer.Channel
                    + df_customer.Division
                    + df_customer.CustCode
                ).values,
                df_customer.CustId.values,
            )
        )
        return self._customer_map

    def get_material_map(self) -> dict:
        if self._material_map is not None:
            return self._material_map

        df_materials: pd.DataFrame = self._load_materials()

        # Cache the map for reuse
        self._material_map = dict(
            zip(df_materials.MaterialCode.values, df_materials.MaterialId.values)
        )

        return self._material_map

    def get_agent_map(self) -> dict:
        if self._agent_map is not None:
            return self._agent_map

        df_agents: pd.DataFrame = self._load_agents()

        # Cache the map for reuse
        # Create composite key: AgentType + AgentCode
        self._agent_map = dict(
            zip(
                (df_agents.AgentType + df_agents.AgentCode).values,
                df_agents.AgentId.values,
            )
        )

        return self._agent_map
