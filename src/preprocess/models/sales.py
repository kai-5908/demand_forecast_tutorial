from datetime import datetime, date
from pathlib import Path

import patito
from patito import Field
from pydantic import BaseModel
import polars as pl
from polars import DataFrame

from day_of_week import DAYS_OF_WEEK
from stores import STORES
from items import ITEMS


class SalesDataSchema(patito.Model):
    date: str = Field(regex="\d{4}-\d{2}-\d{2}")
    day_of_week: DAYS_OF_WEEK
    week_of_year: int = Field(ge=1, le=54)
    store: STORES
    item: ITEMS
    item_price: int = Field(ge=0)
    sales: int = Field(ge=0)
    total_sales_amount: int = Field(ge=0)


class SalesData(BaseModel):
    sales_data: DataFrame

    @classmethod
    def from_csv(cls, path: Path) -> "SalesData":
        sales_data = pl.read_csv(source=path)
        sales_data = sales_data
        SalesDataSchema.validate(sales_data)
        sales_data = SalesData(sales_data=sales_data)._cast_date_to_datetime()
        return sales_data

    def query(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        item_name: str | None = None,
        store_name: str | None = None,
    ) -> "SalesData":
        result = self._filter_by_date(date_from=date_from, date_to=date_to)
        result = result._filter_by_item(item_name=item_name)
        result = result._filter_by_store(store_name=store_name)

        return result

    def _cast_date_to_datetime(self) -> "SalesData":
        casted_df = self.sales_data
        casted_df = casted_df.with_columns(pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d").cast(pl.Date))
        return SalesData(sales_data=casted_df)

    def _filter_by_date(
        self, date_from: date | None = None, date_to: date | None = None
    ) -> "SalesData":
        if self.sales_data.get_column("date").dtype is not datetime:
            raise TypeError("Date col is not casted to datetime from str.")
        filtered_df = self.sales_data
        if date_from is not None:
            filtered_df = filtered_df.filter(pl.col("date") >= date_from)
        if date_to is not None:
            filtered_df = filtered_df.filter(pl.col("date") <= date_to)

        return SalesData(sales_data=filtered_df)

    def _filter_by_item(self, item_name: str | None = None) -> "SalesData":
        if self.sales_data.get_column("item").dtype is not str:
            raise TypeError("invalid data type.")
        filtered_df = self.sales_data
        if item_name is not None:
            filtered_df = filtered_df.filter(pl.col("item") == item_name)

        return SalesData(sales_data=filtered_df)

    def _filter_by_store(self, store_name: str | None = None) -> "SalesData":
        if self.sales_data.get_column("store").dtype is not str:
            raise TypeError("invalid data type.")
        filtered_df = self.sales_data
        if store_name is not None:
            filtered_df = filtered_df.filter(pl.col("store") == store_name)

        return SalesData(sales_data=filtered_df)
    
    class Config:
        arbitrary_types_allowed = True
