from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class Fuels_and_refining:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _vw_arbflow_refined_product_arbitrage_endpoint = "/arbflow/arbitrage"

    def get_arbflow_arbitrage(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        from_region: Optional[Union[list[str], Series[str], str]] = None,
        to_region: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        vintage_date: Optional[date] = None,
        vintage_date_lt: Optional[date] = None,
        vintage_date_lte: Optional[date] = None,
        vintage_date_gt: Optional[date] = None,
        vintage_date_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        The Refined Product Arbitrage dataset provides arbitrage information for five refined product commodities across key routes around the world. The dataset includes arbitrage incentive with a breakdown of the key components used to calculate it, enabling users to compare the profitability of sourcing, or supplying refined products by location.

        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             , by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Data Metric Type - Reference column to describe the metrics being reported in the column Value., by default None
         report_for_date: Optional[date], optional
             The date for which the record applies within the data series., by default None
         report_for_date_gt: Optional[date], optional
             filter by `report_for_date > x`, by default None
         report_for_date_gte: Optional[date], optional
             filter by `report_for_date >= x`, by default None
         report_for_date_lt: Optional[date], optional
             filter by `report_for_date < x`, by default None
         report_for_date_lte: Optional[date], optional
             filter by `report_for_date <= x`, by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             Originating Region Name - The name of the geographic area or location from which the commodities originate in a trade or flow., by default None
         to_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region Name - The name of the geographic region where the product is delivered., by default None
         currency: Optional[Union[list[str], Series[str], str]]
             Reported Currency Code - The standardized three-letter code used to identify the currency in which the data is reported., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of how often the data is refreshed or collected., by default None
         vintage_date: Optional[date], optional
             The specific date on which a particular data series was created., by default None
         vintage_date_gt: Optional[date], optional
             filter by `vintage_date > x`, by default None
         vintage_date_gte: Optional[date], optional
             filter by `vintage_date >= x`, by default None
         vintage_date_lt: Optional[date], optional
             filter by `vintage_date < x`, by default None
         vintage_date_lte: Optional[date], optional
             filter by `vintage_date <= x`, by default None
         modified_date: Optional[datetime], optional
             The specific date on which a particular data point was last modified., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
        filter_params.append(list_to_filter("fromRegion", from_region))
        filter_params.append(list_to_filter("toRegion", to_region))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("vintageDate", vintage_date))
        if vintage_date_gt is not None:
            filter_params.append(f'vintageDate > "{vintage_date_gt}"')
        if vintage_date_gte is not None:
            filter_params.append(f'vintageDate >= "{vintage_date_gte}"')
        if vintage_date_lt is not None:
            filter_params.append(f'vintageDate < "{vintage_date_lt}"')
        if vintage_date_lte is not None:
            filter_params.append(f'vintageDate <= "{vintage_date_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/fuels-refining/v1/arbflow/arbitrage",
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

        if "reportForDate" in df.columns:
            df["reportForDate"] = pd.to_datetime(df["reportForDate"])  # type: ignore

        if "vintageDate" in df.columns:
            df["vintageDate"] = pd.to_datetime(df["vintageDate"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "historicalEdgeDate" in df.columns:
            df["historicalEdgeDate"] = pd.to_datetime(df["historicalEdgeDate"])  # type: ignore
        return df
