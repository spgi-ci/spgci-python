
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
        self, country: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, location: Optional[Union[list[str], Series[str], str]] = None, weather_date: Optional[Union[list[date], Series[date], date]] = None, heating_degree_day: Optional[Union[list[str], Series[str], str]] = None, cooling_degree_day: Optional[Union[list[str], Series[str], str]] = None, precipitation_mm: Optional[Union[list[str], Series[str], str]] = None, temp_celsius_high: Optional[Union[list[str], Series[str], str]] = None, temp_celsius_low: Optional[Union[list[str], Series[str], str]] = None, temp_f_high: Optional[Union[list[str], Series[str], str]] = None, temp_f_low: Optional[Union[list[str], Series[str], str]] = None, wind_speed_average_ms: Optional[Union[list[str], Series[str], str]] = None, snowfall_total_cm: Optional[Union[list[str], Series[str], str]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         country: Optional[Union[list[str], Series[str], str]]
             The market in which the weather data is recorded or observed., be default None
         city: Optional[Union[list[str], Series[str], str]]
             The specific city within the country for which the weather data is being reported., be default None
         location: Optional[Union[list[str], Series[str], str]]
             Weather station which provides Differentiator between country weather data and city weather data., be default None
         weather_date: Optional[Union[list[date], Series[date], date]]
             The date for which the weather data is recorded or forecasted., be default None
         heating_degree_day: Optional[Union[list[str], Series[str], str]]
             A measure of how hot the temperature was on a given day. It is calculated as the difference between the daily average temperature and 65 degrees Fahrenheit., be default None
         cooling_degree_day: Optional[Union[list[str], Series[str], str]]
             A measure of how cold the temperature was on a given day. It is calculated as the difference between 65 degrees Fahrenheit and the daily average temperature., be default None
         precipitation_mm: Optional[Union[list[str], Series[str], str]]
             The amount of water measured in millimeters, that falls as rain, snow, sleet, or hail during a specific time period., be default None
         temp_celsius_high: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Celcius., be default None
         temp_celsius_low: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Celcius., be default None
         temp_f_high: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         temp_f_low: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         wind_speed_average_ms: Optional[Union[list[str], Series[str], str]]
             The average wind speed measured in meters per second during a specific time period., be default None
         snowfall_total_cm: Optional[Union[list[str], Series[str], str]]
             The total amount of snowfall in centimeters recorded during a specific time period., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the Weather Actual., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("weather_date", weather_date))
        filter_params.append(list_to_filter("heating_degree_day", heating_degree_day))
        filter_params.append(list_to_filter("cooling_degree_day", cooling_degree_day))
        filter_params.append(list_to_filter("precipitation_mm", precipitation_mm))
        filter_params.append(list_to_filter("temp_celsius_high", temp_celsius_high))
        filter_params.append(list_to_filter("temp_celsius_low", temp_celsius_low))
        filter_params.append(list_to_filter("temp_f_high", temp_f_high))
        filter_params.append(list_to_filter("temp_f_low", temp_f_low))
        filter_params.append(list_to_filter("wind_speed_average_ms", wind_speed_average_ms))
        filter_params.append(list_to_filter("snowfall_total_cm", snowfall_total_cm))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
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
        self, country: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, location: Optional[Union[list[str], Series[str], str]] = None, recorded_date: Optional[Union[list[date], Series[date], date]] = None, weather_date: Optional[Union[list[date], Series[date], date]] = None, day_sequence: Optional[Union[list[int], Series[int], int]] = None, heating_degree_day: Optional[Union[list[str], Series[str], str]] = None, cooling_degree_day: Optional[Union[list[str], Series[str], str]] = None, precipitation_mm: Optional[Union[list[str], Series[str], str]] = None, temp_celsius_high: Optional[Union[list[str], Series[str], str]] = None, temp_celsius_low: Optional[Union[list[str], Series[str], str]] = None, temp_f_high: Optional[Union[list[str], Series[str], str]] = None, temp_f_low: Optional[Union[list[str], Series[str], str]] = None, wind_speed_average_ms: Optional[Union[list[str], Series[str], str]] = None, snowfall_total_cm: Optional[Union[list[str], Series[str], str]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         country: Optional[Union[list[str], Series[str], str]]
             The market in which the weather data is recorded or observed., be default None
         city: Optional[Union[list[str], Series[str], str]]
             The specific city within the country for which the weather data is being reported., be default None
         location: Optional[Union[list[str], Series[str], str]]
             Weather station which provides Differentiator between country weather data and city weather data., be default None
         recorded_date: Optional[Union[list[date], Series[date], date]]
             Recorded Date refers to the specific date on which weather updateds are recorded., be default None
         weather_date: Optional[Union[list[date], Series[date], date]]
             The date for which the weather data is recorded or forecasted., be default None
         day_sequence: Optional[Union[list[int], Series[int], int]]
             The number of days forecasted out from the start of the forecast date. For example, a Day Sequence of 7 would indicate the 7th day of a forecast period., be default None
         heating_degree_day: Optional[Union[list[str], Series[str], str]]
             A measure of how hot the temperature was on a given day. It is calculated as the difference between the daily average temperature and 65 degrees Fahrenheit., be default None
         cooling_degree_day: Optional[Union[list[str], Series[str], str]]
             A measure of how cold the temperature was on a given day. It is calculated as the difference between 65 degrees Fahrenheit and the daily average temperature., be default None
         precipitation_mm: Optional[Union[list[str], Series[str], str]]
             The amount of water measured in millimeters, that falls as rain, snow, sleet, or hail during a specific time period., be default None
         temp_celsius_high: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Celcius., be default None
         temp_celsius_low: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Celcius., be default None
         temp_f_high: Optional[Union[list[str], Series[str], str]]
             The highest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         temp_f_low: Optional[Union[list[str], Series[str], str]]
             The lowest temperatures recorded or forecasted during a specific day, measured in degrees Fahrenheit., be default None
         wind_speed_average_ms: Optional[Union[list[str], Series[str], str]]
             The average wind speed measured in meters per second during a specific time period., be default None
         snowfall_total_cm: Optional[Union[list[str], Series[str], str]]
             The total amount of snowfall in centimeters recorded during a specific time period., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the Weather Forecast., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("recorded_date", recorded_date))
        filter_params.append(list_to_filter("weather_date", weather_date))
        filter_params.append(list_to_filter("day_sequence", day_sequence))
        filter_params.append(list_to_filter("heating_degree_day", heating_degree_day))
        filter_params.append(list_to_filter("cooling_degree_day", cooling_degree_day))
        filter_params.append(list_to_filter("precipitation_mm", precipitation_mm))
        filter_params.append(list_to_filter("temp_celsius_high", temp_celsius_high))
        filter_params.append(list_to_filter("temp_celsius_low", temp_celsius_low))
        filter_params.append(list_to_filter("temp_f_high", temp_f_high))
        filter_params.append(list_to_filter("temp_f_low", temp_f_low))
        filter_params.append(list_to_filter("wind_speed_average_ms", wind_speed_average_ms))
        filter_params.append(list_to_filter("snowfall_total_cm", snowfall_total_cm))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
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
        
        if "weather_date" in df.columns:
            df["weather_date"] = pd.to_datetime(df["weather_date"])  # type: ignore

        if "modified_date" in df.columns:
            df["modified_date"] = pd.to_datetime(df["modified_date"])  # type: ignore

        if "recorded_date" in df.columns:
            df["recorded_date"] = pd.to_datetime(df["recorded_date"])  # type: ignore
        return df
    