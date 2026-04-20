from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import datetime
import pandas as pd


class EUPower:
    _endpoint = "api/v1/"
    _api_eu_power_vision_assets_mv_endpoint = "power-assets"

    def get_power_assets(
        self,
        *,
        plant_name: Optional[Union[list[str], Series[str], str]] = None,
        unit_id: Optional[Union[list[str], Series[str], str]] = None,
        operator: Optional[Union[list[str], Series[str], str]] = None,
        plant_technology: Optional[Union[list[str], Series[str], str]] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        unit_primary_fuel: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        The European Power Assets API provides details of European Power Infrastructure Data.

        Parameters
        ----------
        plant_name      :   Optional[Union[list[str], Series[str], str]] = None,
            filter by plantName, by default None
        unit_id         :   Optional[Union[list[str], Series[str], str]] = None,
            filter by unitId, by default None
        operator        :   Optional[Union[list[str], Series[str], str]] = None,
            filter by operator, by default None
        plant_technology:   Optional[Union[list[str], Series[str], str]] = None,
            filter by plantTechnology, by default None
        status          :   Optional[Union[list[str], Series[str], str]] = None,
            filter by status, by default None
        unit_primary_fuel:  Optional[Union[list[str], Series[str], str]] = None,
            filter by unitPrimaryFuel, by default None
        country         :   Optional[Union[list[str], Series[str], str]] = None,
            filter by country, by default None
        modified_date   :   Optional[datetime] = None,
            filter by ``lastModifiedDate = x`` , by default None
        modified_date_gt:   Optional[datetime] = None,
            filter by ``lastModifiedDate > x`` , by default None
        modified_date_gte:  Optional[datetime] = None,
            filter by ``lastModifiedDate >= x`` , by default None
        modified_date_lt:   Optional[datetime] = None,
            filter by ``lastModifiedDate < x`` , by default None
        modified_date_lte:  Optional[datetime] = None,
            filter by ``lastModifiedDate <= x`` , by default None
        filter_exp      :   Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page            :   int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size       :   int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 5000
        paginate        :   bool, optional
            whether to auto-paginate the response, by default False
        raw             :   bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
              DataFrame
                  DataFrame of the ``response.json()``
              Response
                  Raw ``requests.Response`` object

        Examples
        --------
        **Simple**
        >>> power.get_power_assets(plant_technology="Solar", paginate=True)

        **Filter by multiple fields**
        >>> power.get_power_assets(country="Spain", status="Operating")

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("plantName", plant_name))
        filter_params.append(list_to_filter("unitId", unit_id))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("plantTechnology", plant_technology))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("unitPrimaryFuel", unit_primary_fuel))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("lastModifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path="egp/v1/eupower/power-assets",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore

        if "lastModifiedDate" in df.columns:
            df["lastModifiedDate"] = pd.to_datetime(df["lastModifiedDate"])  # type: ignore
        return df