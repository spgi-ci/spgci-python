
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Weather:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _lng_weather_actual_endpoint = "/actual"
    _lng_weather_forecast_endpoint = "/forecast"


    def get_actual(
        self, market: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, location: Optional[Union[list[str], Series[str], str]] = None, weatherDate: Optional[Union[list[date], Series[date], date]] = None, heatingDegreeDay: Optional[Union[list[str], Series[str], str]] = None, coolingDegreeDay: Optional[Union[list[str], Series[str], str]] = None, precipitationMm: Optional[Union[list[str], Series[str], str]] = None, tempCelsiusHigh: Optional[Union[list[str], Series[str], str]] = None, tempCelsiusLow: Optional[Union[list[str], Series[str], str]] = None, tempFahrenheitHigh: Optional[Union[list[str], Series[str], str]] = None, tempFahrenheitLow: Optional[Union[list[str], Series[str], str]] = None, windSpeedAverageMs: Optional[Union[list[str], Series[str], str]] = None, snowfallTotalCm: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         market: Optional[Union[list[str], Series[str], str]]
             The market in which the weather data is recorded or observed., be default None
         city: Optional[Union[list[str], Series[str], str]]
             The specific city within the country for which the weather data is being reported., be default None
         location: Optional[Union[list[str], Series[str], str]]
             Weather station which provides Differentiator between country weather data and city weather data., be default None
         weatherDate: Optional[Union[list[date], Series[date], date]]
             The date for which the weather data is recorded or forecasted., be default None
         heatingDegreeDay: Optional[Union[list[str], Series[str], str]]
             A measure of how hot the temperature was on a given day. It is calculated as the difference between the daily average temperature and 65 degrees Fahrenheit., be default None
         coolingDegreeDay: Optional[Union[list[str], Series[str], str]]
             A measure of how cold the temperature was on a given day. It is calculated as the difference between 65 degrees Fahrenheit and the daily average temperature., be default None
         precipitationMm: Optional[Union[list[str], Series[str], str]]
             The amount of water measured in millimeters, that falls as rain, snow, sleet, or hail during a specific time period., be default None
         tempCelsiusHigh: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Celsius., be default None
         tempCelsiusLow: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Celsius., be default None
         tempFahrenheitHigh: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         tempFahrenheitLow: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         windSpeedAverageMs: Optional[Union[list[str], Series[str], str]]
             The average wind speed measured in meters per second during a specific time period., be default None
         snowfallTotalCm: Optional[Union[list[str], Series[str], str]]
             The total amount of snowfall in centimeters recorded during a specific time period., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the Weather Actual., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("weatherDate", weatherDate))
        filter_params.append(list_to_filter("heatingDegreeDay", heatingDegreeDay))
        filter_params.append(list_to_filter("coolingDegreeDay", coolingDegreeDay))
        filter_params.append(list_to_filter("precipitationMm", precipitationMm))
        filter_params.append(list_to_filter("tempCelsiusHigh", tempCelsiusHigh))
        filter_params.append(list_to_filter("tempCelsiusLow", tempCelsiusLow))
        filter_params.append(list_to_filter("tempFahrenheitHigh", tempFahrenheitHigh))
        filter_params.append(list_to_filter("tempFahrenheitLow", tempFahrenheitLow))
        filter_params.append(list_to_filter("windSpeedAverageMs", windSpeedAverageMs))
        filter_params.append(list_to_filter("snowfallTotalCm", snowfallTotalCm))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/weather/v1/actual",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_forecast(
        self, market: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, location: Optional[Union[list[str], Series[str], str]] = None, recordedDate: Optional[Union[list[date], Series[date], date]] = None, weatherDate: Optional[Union[list[date], Series[date], date]] = None, daySequence: Optional[Union[list[str], Series[str], str]] = None, heatingDegreeDay: Optional[Union[list[str], Series[str], str]] = None, coolingDegreeDay: Optional[Union[list[str], Series[str], str]] = None, precipitationMm: Optional[Union[list[str], Series[str], str]] = None, tempCelsiusHigh: Optional[Union[list[str], Series[str], str]] = None, tempCelsiusLow: Optional[Union[list[str], Series[str], str]] = None, tempFahrenheitHigh: Optional[Union[list[str], Series[str], str]] = None, tempFahrenheitLow: Optional[Union[list[str], Series[str], str]] = None, windSpeedAverageMs: Optional[Union[list[str], Series[str], str]] = None, snowfallTotalCm: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         market: Optional[Union[list[str], Series[str], str]]
             The market in which the weather data is recorded or observed., be default None
         city: Optional[Union[list[str], Series[str], str]]
             The specific city within the country for which the weather data is being reported., be default None
         location: Optional[Union[list[str], Series[str], str]]
             Weather station which provides Differentiator between country weather data and city weather data., be default None
         recordedDate: Optional[Union[list[date], Series[date], date]]
             Recorded Date refers to the specific date on which weather updateds are recorded., be default None
         weatherDate: Optional[Union[list[date], Series[date], date]]
             The date for which the weather data is recorded or forecasted., be default None
         daySequence: Optional[Union[list[str], Series[str], str]]
             The number of days forecasted out from the start of the forecast date. For example, a Day Sequence of 7 would indicate the 7th day of a forecast period., be default None
         heatingDegreeDay: Optional[Union[list[str], Series[str], str]]
             A measure of how hot the temperature was on a given day. It is calculated as the difference between the daily average temperature and 65 degrees Fahrenheit., be default None
         coolingDegreeDay: Optional[Union[list[str], Series[str], str]]
             A measure of how cold the temperature was on a given day. It is calculated as the difference between 65 degrees Fahrenheit and the daily average temperature., be default None
         precipitationMm: Optional[Union[list[str], Series[str], str]]
             The amount of water measured in millimeters, that falls as rain, snow, sleet, or hail during a specific time period., be default None
         tempCelsiusHigh: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Celsius., be default None
         tempCelsiusLow: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Celsius., be default None
         tempFahrenheitHigh: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         tempFahrenheitLow: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         windSpeedAverageMs: Optional[Union[list[str], Series[str], str]]
             The average wind speed measured in meters per second during a specific time period., be default None
         snowfallTotalCm: Optional[Union[list[str], Series[str], str]]
             The total amount of snowfall in centimeters recorded during a specific time period., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the Weather Forecast., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("recordedDate", recordedDate))
        filter_params.append(list_to_filter("weatherDate", weatherDate))
        filter_params.append(list_to_filter("daySequence", daySequence))
        filter_params.append(list_to_filter("heatingDegreeDay", heatingDegreeDay))
        filter_params.append(list_to_filter("coolingDegreeDay", coolingDegreeDay))
        filter_params.append(list_to_filter("precipitationMm", precipitationMm))
        filter_params.append(list_to_filter("tempCelsiusHigh", tempCelsiusHigh))
        filter_params.append(list_to_filter("tempCelsiusLow", tempCelsiusLow))
        filter_params.append(list_to_filter("tempFahrenheitHigh", tempFahrenheitHigh))
        filter_params.append(list_to_filter("tempFahrenheitLow", tempFahrenheitLow))
        filter_params.append(list_to_filter("windSpeedAverageMs", windSpeedAverageMs))
        filter_params.append(list_to_filter("snowfallTotalCm", snowfallTotalCm))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/weather/v1/forecast",
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
        
        if "weatherDate" in df.columns:
            df["weatherDate"] = pd.to_datetime(df["weatherDate"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "recordedDate" in df.columns:
            df["recordedDate"] = pd.to_datetime(df["recordedDate"])  # type: ignore
        return df
    
