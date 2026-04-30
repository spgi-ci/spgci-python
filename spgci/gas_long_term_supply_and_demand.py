from __future__ import annotations
from typing import List, Optional, Union, Literal
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class GasLongTermSupplyAndDemand:
    _endpoint = "api/v1/"
    _greater_china_endpoint = "apac/greater-china"
    _oecd_endpoint = "apac/oecd"
    _sasea_endpoint = "apac/sasea"

    _datasets = Literal[
        "greater-china",
        "oecd",
        "sasea",
        "europe",
        "latin-america",
    ]

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a dataset, optionally filtered by an expression.

        This method is crucial for data discovery and validation before making actual data queries.
        Use this to understand what values are available in the dataset and what combinations
        actually exist before attempting to filter your main data queries.

        Args:
            dataset (str): The APAC gas dataset name:
                - "greater-china"
                - "oecd"
                - "sasea"
            columns (list[str] or str): Column names to get unique values for.
                - Use camelCase format: ["commodity", "geography", "year"]
                - Can be single string: "commodity"
                - Can be multiple columns: ["commodity", "geography", "year"]
            filter_exp (str, optional): Filter expression to limit results to specific subsets.
                Use ci.utilities.build_filter_expression() to construct this properly.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns,
            optionally filtered by the provided expression.

        
        **Example usage**
            >>> apac_gas.get_unique_values("greater-china", "commodity")
            
        """
        dataset_to_path = {
            "greater-china": "analytics/gas/v1/apac/greater-china",
            "oecd": "analytics/gas/v1/apac/oecd",
            "sasea": "analytics/gas/v1/apac/sasea",
            "europe": "analytics/gas/v1/emea/europe",
            "latin-america": "analytics/gas/v1/amer/latin-america",

        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(
                f"dataset '{dataset}' not found ",
            )
        else:
            path = dataset_to_path[dataset]

        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"groupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)


    def get_greater_china(
        self,
        *,
        subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
        commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
        geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
        uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
        frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
        category: Optional[Union[List[str], str, date, datetime, Series]] = None,
        as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        year: Optional[Union[List[str], str, date, datetime, Series]] = None,
        modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        select: Optional[Union[List[str], str]] = None,
        sort: Optional[Union[List[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to historical data and long‑term forecasts of natural gas supply and demand across Greater China.

        Parameters
        ----------

         subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
         commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
         geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
         uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
         frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
         category: Optional[Union[List[str], str, date, datetime, Series]] = None,
         as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         year: Optional[Union[List[str], str, date, datetime, Series]] = None,
         modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         select: Optional[Union[List[str], str]] = None,
         sort: Optional[Union[List[str], str]] = None,
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("asOfDate", as_of_date))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if select is not None:
            params["select"] = ",".join(select) if isinstance(select, list) else select
        if sort is not None:
            params["sort"] = ",".join(sort) if isinstance(sort, list) else sort

        response = get_data(
            path="analytics/gas/v1/apac/greater-china",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_oecd(
        self,
        *,
        subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
        commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
        geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
        uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
        frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
        category: Optional[Union[List[str], str, date, datetime, Series]] = None,
        as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        year: Optional[Union[List[str], str, date, datetime, Series]] = None,
        modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        select: Optional[Union[List[str], str]] = None,
        sort: Optional[Union[List[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to historical data and long‑term forecasts of natural gas supply and demand across OECD Asia-Pacific.

        Parameters
        ----------

         subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
         commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
         geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
         uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
         frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
         category: Optional[Union[List[str], str, date, datetime, Series]] = None,
         as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         year: Optional[Union[List[str], str, date, datetime, Series]] = None,
         modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         select: Optional[Union[List[str], str]] = None,
         sort: Optional[Union[List[str], str]] = None,
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("asOfDate", as_of_date))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if select is not None:
            params["select"] = ",".join(select) if isinstance(select, list) else select
        if sort is not None:
            params["sort"] = ",".join(sort) if isinstance(sort, list) else sort

        response = get_data(
            path="analytics/gas/v1/apac/oecd",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_sasea(
        self,
        *,
        subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
        commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
        geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
        uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
        frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
        category: Optional[Union[List[str], str, date, datetime, Series]] = None,
        as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        year: Optional[Union[List[str], str, date, datetime, Series]] = None,
        modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        select: Optional[Union[List[str], str]] = None,
        sort: Optional[Union[List[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to historical data and long‑term forecasts of natural gas supply and demand across key markets in South Asia and Southeast Asia.

        Parameters
        ----------

         subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
         commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
         geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
         uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
         frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
         category: Optional[Union[List[str], str, date, datetime, Series]] = None,
         as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         year: Optional[Union[List[str], str, date, datetime, Series]] = None,
         modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         select: Optional[Union[List[str], str]] = None,
         sort: Optional[Union[List[str], str]] = None,
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("asOfDate", as_of_date))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if select is not None:
            params["select"] = ",".join(select) if isinstance(select, list) else select
        if sort is not None:
            params["sort"] = ",".join(sort) if isinstance(sort, list) else sort

        response = get_data(
            path="analytics/gas/v1/apac/sasea",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_europe(
        self,
        *,
        subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
        commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
        geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
        uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
        frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
        category: Optional[Union[List[str], str, date, datetime, Series]] = None,
        as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        year: Optional[Union[List[str], str, date, datetime, Series]] = None,
        modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        select: Optional[Union[List[str], str]] = None,
        sort: Optional[Union[List[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to historical data and long‑term forecasts of natural gas supply and demand across key markets in Europe.

        Parameters
        ----------

         subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
         commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
         geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
         uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
         frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
         category: Optional[Union[List[str], str, date, datetime, Series]] = None,
         as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         year: Optional[Union[List[str], str, date, datetime, Series]] = None,
         modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         select: Optional[Union[List[str], str]] = None,
         sort: Optional[Union[List[str], str]] = None,
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("asOfDate", as_of_date))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if select is not None:
            params["select"] = ",".join(select) if isinstance(select, list) else select
        if sort is not None:
            params["sort"] = ",".join(sort) if isinstance(sort, list) else sort

        response = get_data(
            path="analytics/gas/v1/emea/europe",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_latin_america(
        self,
        *,
        subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
        commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
        geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
        uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
        frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
        category: Optional[Union[List[str], str, date, datetime, Series]] = None,
        as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        year: Optional[Union[List[str], str, date, datetime, Series]] = None,
        modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
        select: Optional[Union[List[str], str]] = None,
        sort: Optional[Union[List[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to historical data and long‑term forecasts of natural gas supply and demand across key markets in Latin America.

        Parameters
        ----------

         subject_area: Optional[Union[List[str], str, date, datetime, Series]] = None,
         commodity: Optional[Union[List[str], str, date, datetime, Series]] = None,
         geography: Optional[Union[List[str], str, date, datetime, Series]] = None,
         uom: Optional[Union[List[str], str, date, datetime, Series]] = None,
         frequency: Optional[Union[List[str], str, date, datetime, Series]] = None,
         category: Optional[Union[List[str], str, date, datetime, Series]] = None,
         as_of_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         year: Optional[Union[List[str], str, date, datetime, Series]] = None,
         modified_date: Optional[Union[List[str], str, date, datetime, Series]] = None,
         select: Optional[Union[List[str], str]] = None,
         sort: Optional[Union[List[str], str]] = None,
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("asOfDate", as_of_date))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if select is not None:
            params["select"] = ",".join(select) if isinstance(select, list) else select
        if sort is not None:
            params["sort"] = ",".join(sort) if isinstance(sort, list) else sort

        response = get_data(
            path="analytics/gas/v1/amer/latin-america",
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

        if "asOfDate" in df.columns:
            df["asOfDate"] = pd.to_datetime(df["asOfDate"])  # type: ignore

        if "year" in df.columns:
            df["year"] = pd.to_datetime(df["year"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df