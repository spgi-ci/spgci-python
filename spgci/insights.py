# Copyright 2023 S&P Global Commodity Insights

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from .api_client import get_data, Paginator
from .utilities import list_to_filter
from typing import Union, Optional
from pandas import DataFrame, Series, to_datetime, json_normalize  # type: ignore
import pandas as pd
from distutils.version import LooseVersion
from requests import Response
from datetime import datetime
from enum import Enum
from functools import partial
import html


class Insights:
    """
    Platts Insights.

    Includes
    --------
    ``ContentType`` enum for ``content_type`` for the ``get_stories`` method.\n
    ``SubscriberNotesContentType`` enum for ``content_type`` for the ``get_subscriber_notes`` method.\n
    ``get_stories`` get articles across all content types.\n
    ``get_top_news`` get articles that are considered "Top News".\n
    ``get_latest_news`` get articles that are considered "Latest News".\n
    ``get_spotlights`` get articles that are considered "Spotlights".\n
    ``get_heards`` get heards, assessments summaries, market information summaries and tenders.\n
    ``get_subscriber_notes`` get subscriber notes.\n
    ``get_content`` get insights by ID.\n

    """

    _path = "news-insights"

    class HeardsContentType(Enum):
        """"""

        Tenders = "Tenders"
        Heard = "Heard"
        AssessmentSummary = "Assessment Summary"
        MarketInformationSummary = "Market Information Summary"

    class ContentType(Enum):
        """Content Type"""

        Analytics = "Analytics"
        PortCommentary = "Port Commentary"
        News = "News"
        Summary = "Summary"
        Promo = "Promo"
        Blog = "Blog"
        Rationale = "Rationale"
        Analysis = "Analysis"
        TraderNote = "Trader Note"
        MarketCommentary = "Market Commentary"
        Feature = "Feature"
        Factbox = "Factbox"

    class SubscriberNotesContentType(Enum):
        """Subscriber Notes Content Type"""

        DataCorrection = "Data Correction"
        ProductFormatChange = "Product Format Change"
        MethodologyNote = "Methodology Note"
        MOCParticipationNote = "MOC Participation Note"
        MarketNotification = "Market Notification"
        HolidayNotice = "HolidayNotice"

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["total_pages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    @staticmethod
    def _to_df(resp: Response, strip_html: bool) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if len(df) > 0:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["updatedDate"] = to_datetime(
                    df["updatedDate"], utc=True, format="ISO8601"
                )
            else:
                df["updatedDate"] = to_datetime(df["updatedDate"], utc=True)

        if strip_html:
            if "headline" in df.columns:
                df["headline"] = df["headline"].str.replace(r"<.*?>", " ", regex=True)  # type: ignore
                df["headline"] = df["headline"].apply(lambda s: html.unescape(str(s)).replace("\n", " "))  # type: ignore
            if "body" in df.columns:
                df["body"] = df["body"].str.replace(r"<.*?>", " ", regex=True)  # type: ignore
                df["body"] = df["body"].apply(lambda s: html.unescape(str(s)).replace("\n", " "))  # type: ignore
            if "lead" in df.columns:
                df["lead"] = df["lead"].str.replace(r"<.*?>", " ", regex=True)  # type: ignore
                df["lead"] = df["lead"].apply(lambda s: html.unescape(str(s)).replace("\n", " "))  # type: ignore

        return df

    @staticmethod
    def _content_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = json_normalize(j["envelope"])

        return df

    def get_heards(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[
                list[str],
                "Series[str]",
                list[HeardsContentType],
                str,
                HeardsContentType,
            ]
        ] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch written content like Market Commentaries, Rationales, Spotlights, Market Information and Assessment Summaries, Publication reports.

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[HeardsContentType], str, HeardsContentType]], optional
            filter by contentType, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_heards(q="Suez")

        **Stripping HTML Tags**
        >>> ci.Insights().get_heards(q="Suez", strip_html=True)

        **Including other fields**
        >>> ci.Insights().get_heards(q="Suez", field="body,commodity,geography")

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_heards(page_number=["0300", "0100"], service_line="PCA", updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_heards(content_type=ci.Insights.HeardsContentType.Tenders)
        """
        path = "/v1/search/heards"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("contentType", content_type))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_latest_news(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[list[str], "Series[str]", list[ContentType], str, ContentType]
        ] = None,
        subject_area: Optional[Union[list[str], "Series[str]", str]] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "lead,body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch stories limited to "Latest News"

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[ContentType], str, ContentType]], optional
            filter by contentType, by default None
        subject_area : Optional[Union[list[str], Series[str], str]], optional
            filter by subjectArea, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_latest_news(q="Suez")

        **Stripping HTML Tags**
        >>> ci.Insights().get_latest_news(q="Suez", strip_html=True)

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_latest_news(geography=["Middle East", "Asia"], updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_latest_news(content_type=[Insights.ContentType.News, Insights.ContentType.MarketCommentary])
        """
        path = "/v1/search/story/latest-news"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("contentType", content_type))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))
        filter_params.append(list_to_filter("subjectArea", subject_area))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_spotlights(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[list[str], "Series[str]", list[ContentType], str, ContentType]
        ] = None,
        subject_area: Optional[Union[list[str], "Series[str]", str]] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "lead,body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch stories limited to "Spotlights"

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[ContentType], str, ContentType]], optional
            filter by contentType, by default None
        subject_area : Optional[Union[list[str], Series[str], str]], optional
            filter by subjectArea, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_spotlights(q="Suez")

        **Stripping HTML Tags**
        >>> ci.Insights().get_spotlights(q="Suez", strip_html=True)

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_spotlights(geography=["Middle East", "Asia"], updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_spotlights(content_type=[Insights.ContentType.News, Insights.ContentType.MarketCommentary])
        """
        path = "/v1/search/story/spotlights"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("contentType", content_type))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_top_news(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[list[str], "Series[str]", list[ContentType], str, ContentType]
        ] = None,
        subject_area: Optional[Union[list[str], "Series[str]", str]] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "lead,body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch stories limited to "Top News"

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[ContentType], str, ContentType]], optional
            filter by contentType, by default None
        subject_area : Optional[Union[list[str], Series[str], str]], optional
            filter by subjectArea, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_top_news(q="Suez")

        **Stripping HTML Tags**
        >>> ci.Insights().get_top_news(q="Suez", strip_html=True)

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_top_news(geography=["Middle East", "Asia"], updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_top_news(content_type=[Insights.ContentType.News, Insights.ContentType.MarketCommentary])
        """
        path = "/v1/search/story/top-news"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("contentType", content_type))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_stories(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[list[str], "Series[str]", list[ContentType], str, ContentType]
        ] = None,
        subject_area: Optional[Union[list[str], "Series[str]", str]] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "lead,body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch written content like Market Commentaries, Rationales, Spotlights, Market Information and Assessment Summaries, Publication reports.

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[ContentType], str, ContentType]], optional
            filter by contentType, by default None
        subject_area : Optional[Union[list[str], Series[str], str]], optional
            filter by subjectArea, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_stories(q="Suez")

        **Stripping HTML Tags**
        >>> ci.Insights().get_stories(q="Suez", strip_html=True)

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_stories(geography=["Middle East", "Asia"], updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_stories(content_type=[Insights.ContentType.News, Insights.ContentType.MarketCommentary])
        """
        path = "/v1/search/story"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("contentType", content_type))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_subscriber_notes(
        self,
        *,
        q: Optional[str] = None,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        service_line: Optional[Union[list[str], "Series[str]", str]] = None,
        page_number: Optional[Union[list[str], "Series[str]", str]] = None,
        pricing_region: Optional[Union[list[str], "Series[str]", str]] = None,
        publication: Optional[Union[list[str], "Series[str]", str]] = None,
        content_type: Optional[
            Union[
                list[str],
                "Series[str]",
                list[SubscriberNotesContentType],
                str,
                SubscriberNotesContentType,
            ]
        ] = None,
        company: Optional[Union[list[str], "Series[str]", str]] = None,
        subject_area: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date: Optional[datetime] = None,
        updated_date_gt: Optional[datetime] = None,
        updated_date_gte: Optional[datetime] = None,
        updated_date_lt: Optional[datetime] = None,
        updated_date_lte: Optional[datetime] = None,
        field: Optional[str] = "lead,body",
        strip_html: bool = False,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch Subscriber Notes such as changes in assessment coverage, methodology updates, and updates to publishing schedules.

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        service_line : Optional[Union[list[str], Series[str], str]], optional
            filter by serviceLine, by default None
        page_number : Optional[Union[list[str], Series[str], str]], optional
            filter by pageNumber, by default None
        pricing_region : Optional[Union[list[str], Series[str], str]], optional
            filter by pricingRegion, by default None
        publication : Optional[Union[list[str], Series[str], str]], optional
            filter by publication, by default None
        content_type : Optional[Union[list[str], Series[str], list[ContentType], str, ContentType]], optional
            filter by contentType, by default None
        subject_area : Optional[Union[list[str], Series[str], str]], optional
            filter by subjectArea, by default None
        company : Optional[Union[list[str], Series[str], str]], optional
            filter by company, by default None
        updated_date : Optional[datetime], optional
            filter by ``updatedDate = x`` , by default None
        updated_date_gt : Optional[datetime], optional
            filter by ``updatedDate > x`` , by default None
        updated_date_gte : Optional[datetime], optional
            filter by ``updatedDate >= x`` , by default None
        updated_date_lt : Optional[datetime], optional
            filter by ``updatedDate < x`` , by default None
        updated_date_lte : Optional[datetime], optional
            filter by ``updatedDate <= x`` , by default None
        field : Optional[str], optional
            pass-thru ``field`` query param to select which columns to return, by default "lead,body"
        strip_html : bool, optional
            remove html tags, encoding and ``\n`` from ``headline``, ``body`` and ``lead`` , by default False
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Free text search**
        >>> ci.Insights().get_subscriber_notes(q="steel")

        **Stripping HTML Tags**
        >>> ci.Insights().get_subscriber_notes(q="steel", strip_html=True)

        **Using List**
        >>> from datetime import datetime
        >>> ci.Insights().get_subscriber_notes(geography=["Middle East", "Asia"], updated_date_gte=datetime(2023,3,1))

        **Using Enum**
        >>> ci.Insights().get_subscriber_notes(content_type=[ci.Insights.SubscriberNotesContentType.MethodologyNote, ci.Insights.SubscriberNotesContentType.MarketNotification])
        """
        path = "/v1/search/subscriber-notes"
        filter_params: list[str] = []

        if updated_date_gt != None:
            filter_params.append(f'updatedDate > "{updated_date_gt}"')
        if updated_date_gte != None:
            filter_params.append(f'updatedDate >= "{updated_date_gte}"')
        if updated_date_lt != None:
            filter_params.append(f'updatedDate < "{updated_date_lt}"')
        if updated_date_lte != None:
            filter_params.append(f'updatedDate <= "{updated_date_lte}"')

        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("serviceLine", service_line))
        filter_params.append(list_to_filter("pageNumber", page_number))
        filter_params.append(list_to_filter("pricingRegion", pricing_region))
        filter_params.append(list_to_filter("publication", publication))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("subjectArea", subject_area))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("updatedDate", updated_date))
        filter_params.append(list_to_filter("contentType", content_type))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        else:
            filter_exp += " AND " + " AND ".join(filter_params)

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "q": q,
            "field": field,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=partial(self._to_df, strip_html=strip_html),
        )

    def get_content(
        self,
        id: str,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch a single piece of content with all available fields.

        Parameters
        ----------
        id : str
            filter by id
        raw : bool, optional
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
        **Get By ID**
        >>> ci.Insights().get_content(id="169b5f45-04e4-43f2-b83b-ecc16f96be55")

        """
        path = f"/v1/content/{id}"

        params = {}
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            raw=raw,
            df_fn=self._content_to_df,
        )
