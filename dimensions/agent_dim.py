import pandas as pd
from datetime import date, timedelta
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from sqlalchemy import (
    bindparam,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    update,
    Engine,
    insert,
    text,
    Insert,
    Update,
)


class AgentDim:
    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup ):
        self._con_dw: Engine = con_dw
        self._con_sap: Engine = con_sap
        self._lookup: DimensionLookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self) -> None:
        """
        Process agents from SAP and sync with DW AgentDim table.
        Handles inserts for new agents and updates for existing ones.
        """
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        Logger().info(f"Processing agents for date: {yesterday}")

        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_agents'"
        )
        sql_get_agents = f"""
                            SELECT BPTYPE,
                                   AGENTCODE,
                                   AGENTNAME,
                                   MODDATE
                            FROM SAPSR3.ZCON_V_AGENTES
                            WHERE MODDATE = '{yesterday}'
                        """
        
        results: pd.DataFrame = pd.read_sql(
            text(sql_get_agents), self._con_sap, params={"yesterday": yesterday}
        )

        if results.empty:
            Logger().info("No agents found for processing")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return
        
        # normalize column names to lowercase to make downstream accesses predictable
        results.columns = results.columns.str.lower()

        # Define the table
        metadata: MetaData = MetaData()

        # Define AgentDim Table
        agent_table: Table = Table(
            self._config.TABLE_AGENT_DIM,
            metadata,
            Column("AgentId", Integer, primary_key=True),
            Column("AgentType", String(10)),
            Column("AgentCode", String(100)),
            Column("AgentName", String),
        )

        stmt_insert_agents: Insert = insert(agent_table)

        stmt_update_agents: Update = (
            update(agent_table)
            .where(agent_table.c.AgentId == bindparam("b_AgentId"))
            .values(AgentName=bindparam("b_AgentName"))
        )
        
        with self._con_dw.begin() as conn:
            agent_map = self._lookup.get_agent_map()

            # Add search key for lookups
            results["search_key"] = results["bptype"] + results["agentcode"]
            results["AgentId"] = results["search_key"].map(agent_map)

            # Split into updates and inserts
            updates_df = results[results["AgentId"].notna()]
            inserts_df = results[results["AgentId"].isna()]

            if not updates_df.empty:
                update_data = updates_df.rename(
                    columns={"agentname": "b_AgentName", "AgentId": "b_AgentId"}
                )[["b_AgentName", "b_AgentId"]].to_dict(orient="records")
                conn.execute(stmt_update_agents, update_data)  # type: ignore

            if not inserts_df.empty:
                insert_data = inserts_df.rename(
                    columns={
                        "agentcode": "AgentCode",
                        "bptype": "AgentType",
                        "agentname": "AgentName",
                    }
                )[["AgentCode", "AgentType", "AgentName"]].to_dict(orient="records")
                conn.execute(stmt_insert_agents, insert_data)  # type: ignore
            
            conn.execute(stmt_update_etl)
