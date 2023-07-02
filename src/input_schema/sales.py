from datetime import datetime, date
from pathlib import Path

import patito
from patito import Field
from pydantic import BaseModel
import polars as pl
from polars import DataFrame

DAYS_OF_WEEK = [
    "MON",
    "TUE",
    "WED",
    "THU",
    "FRI",
    "SAT",
    "SUN",
]

STORES = [
    "nagoya",
    "shinjuku",
    "osaka",
    "kobe",
    "sendai",
    "chiba",
    "morioka",
    "ginza",
    "yokohama",
    "ueno",
]

ITEMS = [
    "fruit_juice",
    "apple_juice",
    "orange_juice",
    "sports_drink",
    "coffee",
    "milk",
    "mineral_water",
    "sparkling_water",
    "soy_milk",
    "beer",
]



class SalesDataSchema(patito.Model):
    date: str = Field(regex="\d{4}-\d{2}-\d{2}")
    day_of_week: DAYS_OF_WEEK
    week_of_year: str = Field(ge=1, le=54)
    store: STORES
    item: ITEMS
    item_price: int = Field(ge=0)
    sales: int = Field(ge=0)
    total_sales_amount: int = Field(ge=0)



class SalesData(BaseModel):
    sales_data: DataFrame

    @classmethod
    def from_csv(cls, path: Path) -> "SalesData":
        sales_data = pl.read_csv(path=path)
        SalesDataSchema.validate(sales_data)
        return SalesData(sales_data=sales_data)
    
    def cast_date_to_datetime(self) -> DataFrame:
        casted_df = self.sales_data
        casted_date_col = self.sales_data.get_column("date").str.strptime(pl.Datetime)
        casted_df.with_columns(casted_date_col)
        return casted_df
    
    def filter_by_date(self, date_from: date | None = None, date_to: date | None = None) -> DataFrame:
        if self.sales_data.get_column("date").dtype is not datetime:
            raise TypeError("Date col is not casted to datetime from str.")
        filtered_df = self.sales_data
        if date_from is not None:
            filtered_df = filtered_df.filter(pl.col("date") >= date_from)
        if date_to is not None:
            filtered_df = filtered_df.filter(pl.col("date") <= date_to)
        
        return filtered_df


    

