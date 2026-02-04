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
    _country_map: dict | None
    _group_map: dict | None
    _zone_map: dict | None
    _region_map: dict | None
    _cust_division_map: dict | None
    _gamma_map: dict | None
    _division_map: dict | None
    _family_map: dict | None
    _subfamily_map: dict | None
    _material_status_map: dict | None
    _vendor_map: dict | None

    def __init__(self, con_dw: Engine):
        self._con_dw: Engine = con_dw
        self._customer_map = None
        self._material_map = None
        self._agent_map = None
        self._country_map = None
        self._group_map = None
        self._zone_map = None
        self._region_map = None
        self._cust_division_map = None
        self._gamma_map = None
        self._division_map = None
        self._family_map = None
        self._subfamily_map = None
        self._material_status_map = None
        self._vendor_map = None
        self._production_area_map = None

    def invalidate_caches(self) -> None:
        self._customer_map = None
        self._material_map = None
        self._agent_map = None
        self._country_map = None
        self._group_map = None
        self._zone_map = None
        self._division_map = None
        self._region_map = None
        self._cust_division_map = None
        self._gamma_map = None
        self._division_map = None
        self._family_map = None
        self._subfamily_map = None
        self._material_status_map = None
        self._vendor_map = None
        self._material_map = None
        self._production_area_map = None

    def invalidate_vendor_cache(self) -> None:
        self._vendor_map = None

    def _load_production_areas(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading production areas from DB")
        query = "SELECT PlantId, ProductionAreaCode, ProductionAreaId FROM ProductionAreaDim"
        return pd.read_sql(query, self._con_dw)

    def _load_vendors(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading vendors from DB")
        vendor_query = "SELECT VendId, VendCode FROM VendorDim"
        return pd.read_sql(vendor_query, self._con_dw)

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

    def _load_countries(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading countries from DB")
        country_query = """SELECT CountryId, CountryCode FROM CountryDim"""
        return pd.read_sql(country_query, self._con_dw)

    def _load_groups(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading groups from DB")
        group_query = "SELECT GroupCode, GroupId FROM CustGroupDim"
        return pd.read_sql(group_query, self._con_dw)

    def _load_zones(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading zones from DB")
        zone_query = """SELECT ZoneId, ZoneCode FROM CustZoneDim"""
        return pd.read_sql(zone_query, self._con_dw)

    def _load_regions(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading regions from DB")
        region_query = """SELECT CountryId,RegionId, RegionCode FROM RegionDim"""
        return pd.read_sql(region_query, self._con_dw)

    def _load_cust_divisions(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading cust_divisions from DB")
        cust_division_query = (
            """SELECT CustDivisionId, CustDivisionCode FROM CustDivisionDim"""
        )
        return pd.read_sql(cust_division_query, self._con_dw)

    def _load_gammas(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading gammas from DB")
        gamma_query = "SELECT GammaCode, GammaId FROM GAMMADIM"
        return pd.read_sql(gamma_query, self._con_dw)

    def _load_divisions(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading divisions from DB")
        division_query = "SELECT GammaCode, DivisionCode, DivisionId FROM DIVISIONDIM"
        return pd.read_sql(division_query, self._con_dw)

    def _load_families(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading families from DB")
        family_query = (
            "SELECT GammaCode, DivisionCode, FamilyCode, FamilyId FROM FAMILYDIM"
        )
        return pd.read_sql(family_query, self._con_dw)

    def _load_subfamilies(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading subfamilies from DB")
        subfamily_query = "SELECT GammaCode, DivisionCode, FamilyCode, SubFamilyCode, SubFamilyId FROM SUBFAMILYDIM"
        return pd.read_sql(subfamily_query, self._con_dw)

    def _load_material_statuses(self) -> pd.DataFrame:
        Logger().info("Cache --> Loading material statuses from DB")
        status_query = (
            "SELECT MaterialStatusId, MaterialStatusCode FROM MaterialStatusDim"
        )
        return pd.read_sql(status_query, self._con_dw)

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

    def get_country_map(self) -> dict:
        if self._country_map is not None:
            return self._country_map

        df_country: pd.DataFrame = self._load_countries()

        # Cache the map for reuse
        self._country_map = dict(
            zip(df_country.CountryCode.values, df_country.CountryId.values)
        )

        return self._country_map

    def get_group_map(self) -> dict:
        if self._group_map is not None:
            return self._group_map

        df_group: pd.DataFrame = self._load_groups()

        # Cache the map for reuse
        self._group_map = dict(zip(df_group.GroupCode.values, df_group.GroupId.values))

        return self._group_map

    def get_zone_map(self) -> dict:
        if self._zone_map is not None:
            return self._zone_map

        df_zone: pd.DataFrame = self._load_zones()

        # Cache the map for reuse
        self._zone_map = dict(zip(df_zone.ZoneCode.values, df_zone.ZoneId.values))

        return self._zone_map

    def get_region_map(self) -> dict:
        if self._region_map is not None:
            return self._region_map

        df_region: pd.DataFrame = self._load_regions()

        # Cache the map for reuse
        self._region_map = dict(
            zip(
                (df_region.CountryId.astype(str) + df_region.RegionCode).values,
                df_region.RegionId.values,
            )
        )

        return self._region_map

    def get_cust_division_map(self) -> dict:
        if self._cust_division_map is not None:
            return self._cust_division_map

        df_cust_division: pd.DataFrame = self._load_cust_divisions()

        # Cache the map for reuse
        self._cust_division_map = dict(
            zip(
                df_cust_division.CustDivisionCode.values,
                df_cust_division.CustDivisionId.values,
            )
        )

        return self._cust_division_map

    def get_gamma_map(self) -> dict:
        if self._gamma_map is not None:
            return self._gamma_map

        df_gamma = self._load_gammas()
        self._gamma_map = dict(zip(df_gamma.GammaCode.values, df_gamma.GammaId.values))
        return self._gamma_map

    def get_division_map(self) -> dict:
        if self._division_map is not None:
            return self._division_map

        df_division = self._load_divisions()
        self._division_map = dict(
            zip(
                (df_division.GammaCode + df_division.DivisionCode).values,
                df_division.DivisionId.values,
            )
        )
        return self._division_map

    def get_family_map(self) -> dict:
        if self._family_map is not None:
            return self._family_map

        df_family = self._load_families()
        self._family_map = dict(
            zip(
                (
                    df_family.GammaCode + df_family.DivisionCode + df_family.FamilyCode
                ).values,
                df_family.FamilyId.values,
            )
        )
        return self._family_map

    def get_subfamily_map(self) -> dict:
        if self._subfamily_map is not None:
            return self._subfamily_map

        df_subfamily = self._load_subfamilies()
        self._subfamily_map = dict(
            zip(
                (
                    df_subfamily.GammaCode
                    + df_subfamily.DivisionCode
                    + df_subfamily.FamilyCode
                    + df_subfamily.SubFamilyCode
                ).values,
                df_subfamily.SubFamilyId.values,
            )
        )
        return self._subfamily_map

    def get_material_status_map(self) -> dict:
        if self._material_status_map is not None:
            return self._material_status_map

        df_status = self._load_material_statuses()
        self._material_status_map = dict(
            zip(df_status.MaterialStatusCode.values, df_status.MaterialStatusId.values)
        )
        return self._material_status_map

    def get_vendor_map(self) -> dict:
        if self._vendor_map is not None:
            return self._vendor_map

        df_vendor = self._load_vendors()
        self._vendor_map = dict(zip(df_vendor.VendCode.values, df_vendor.VendId.values))
        return self._vendor_map

    def get_production_area_map(self) -> dict:
        if self._production_area_map is not None:
            return self._production_area_map

        df_areas = self._load_production_areas()

        # Create composite key: PlantId + ProductionAreaCode
        # Assuming PlantId is string. If not, cast it.
        self._production_area_map = dict(
            zip(
                (df_areas.PlantId.astype(str) + df_areas.ProductionAreaCode).values,
                df_areas.ProductionAreaId.values,
            )
        )
        return self._production_area_map
