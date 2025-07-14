# Copyright 2025 S&P Global Commodity Insights

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Union, Any, TypeVar, Collection, Optional
from pandas import Series
from typing_extensions import TypeGuard
from enum import Enum
from datetime import date

T = TypeVar("T", bound=Enum)


def list_to_filter(
    field_name: str,
    items: Optional[
        Union[
            Collection[str],
            Collection[float],
            Collection[T],
            Collection[date],
            Collection[bool],
            str,
            bool,
            date,
            float,
            T,
        ]
    ] = None,
    strategy: str = "platts",
) -> str:
    single_quote = "'"
    double_quote = '"'
    delim = ":" if strategy == "platts" else " eq"
    quote = f"{double_quote if strategy == 'platts' else single_quote}"
    if isinstance(items, Series):
        return convert_series_to_filterexp(field_name, items, quote)

    if not isinstance(items, (bool)) and not items:
        return ""

    if isinstance(items, bool):
        return f"{field_name}{delim} {items}"
    if isinstance(items, str):
        return f"{field_name}{delim} {quote}{items}{quote}"
    if isinstance(items, Enum):
        return f"{field_name}{delim} {quote}{items.value}{quote}"
    if isinstance(items, float) or isinstance(items, int):
        return f"{field_name}{delim} {items}"
    if isinstance(items, date):
        if strategy == "odata":
            return f"{field_name}{delim} {items}"
        else:
            return f"{field_name}{delim} {quote}{items}{quote}"

    if is_enum_list(items):
        n_items: List[str] = [x.value for x in items]
        n_items = [f"{quote}{x}{quote}" for x in n_items]
    elif is_str_list(items):
        n_items = [f"{quote}{x}{quote}" for x in items]
    elif is_bool_list(items):
        n_items = [f"{x}" for x in items]
    elif is_int_list(items):
        n_items = [str(x) for x in items]
    elif is_date_list(items):
        # if strategy == "odata":
        #     n_items = [str(x) for x in items]
        # else:
        n_items = [f"{quote}{x}{quote}" for x in items]
    else:
        raise TypeError("not supported")

    return f"{field_name} in ({','.join(n_items)})"


def odata_list_to_filter(
    field_name: str,
    items: Optional[
        Union[
            Collection[str],
            Collection[float],
            Collection[T],
            Collection[date],
            str,
            float,
            date,
            T,
        ]
    ] = None,
) -> str:
    return list_to_filter(field_name, items, strategy="odata")


def convert_series_to_filterexp(field_name: str, s: "Series[Any]", quote: str) -> str:
    if is_int_list(s):
        l = [str(x) for x in s]
    elif is_str_list(s):
        l = [f"{quote}{x}{quote}" for x in s]
    elif is_date_list(s):
        l = [f"{quote}{x}{quote}" for x in s]
    else:
        raise TypeError("series type unsupported")

    return f"{field_name} in ({','.join(l)})"


def convert_date_to_filter_exp(
    field_name: str,
    gt_param: Union[date, None],
    gte_param: Union[date, None],
    lt_param: Union[date, None],
    lte_param: Union[date, None],
    filter_params: List[str],
) -> List[str]:
    if gt_param is not None:
        filter_params.append(f'{field_name} > "{gt_param}"')
    if gte_param is not None:
        filter_params.append(f'{field_name} >= "{gte_param}"')
    if lt_param is not None:
        filter_params.append(f'{field_name} < "{lt_param}"')
    if lte_param is not None:
        filter_params.append(f'{field_name} <= "{lte_param}"')

    return filter_params


def is_enum_list(lst: Collection[Any]) -> TypeGuard[List[Enum]]:
    return all(isinstance(x, Enum) for x in lst)


def is_str_list(lst: Collection[Any]) -> TypeGuard[List[str]]:
    return all(isinstance(x, str) for x in lst)


def is_bool_list(lst: Collection[Any]) -> TypeGuard[List[bool]]:
    return all(isinstance(x, bool) for x in lst)


def is_date_list(lst: Collection[Any]) -> TypeGuard[List[date]]:
    return all(isinstance(x, date) for x in lst)


def is_int_list(lst: Collection[Any]) -> TypeGuard[List[float]]:
    return all(isinstance(x, float) for x in lst) or all(
        isinstance(x, int) for x in lst
    )
