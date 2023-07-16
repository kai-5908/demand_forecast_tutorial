from datetime import datetime, date
from pathlib import Path

import patito
from patito import Field
from pydantic import BaseModel, validator
import polars as pl
from polars import DataFrame

from stores import STORES
from items import ITEMS
from sales import SalesData


class WeeklySalesDataSchema(patito.Model):
    year: int
    week_of_year: int = Field(ge=1, le=54)
    month: int = Field(ge=1, le=12)
    store: STORES
    item: ITEMS
    item_price: int = Field(ge=0)
    sales: int = Field(ge=0)
    total_sales_amount: int = Field(ge=0)
    sales_lag_2: int | None
    sales_lag_3: int | None
    sales_lag_4: int | None
    sales_lag_5: int | None
    sales_lag_6: int | None
    sales_lag_7: int | None
    sales_lag_8: int | None
    sales_lag_9: int | None
    sales_lag_10: int | None
    sales_lag_11: int | None
    sales_lag_12: int | None
    sales_lag_13: int | None
    sales_lag_14: int | None
    sales_lag_15: int | None
    sales_lag_16: int | None
    sales_lag_17: int | None
    sales_lag_18: int | None
    sales_lag_19: int | None
    sales_lag_20: int | None
    sales_lag_21: int | None
    sales_lag_22: int | None
    sales_lag_23: int | None
    sales_lag_24: int | None
    sales_lag_25: int | None
    sales_lag_26: int | None
    sales_lag_27: int | None
    sales_lag_28: int | None
    sales_lag_29: int | None
    sales_lag_30: int | None
    sales_lag_31: int | None
    sales_lag_32: int | None
    sales_lag_33: int | None
    sales_lag_34: int | None
    sales_lag_35: int | None
    sales_lag_36: int | None
    sales_lag_37: int | None
    sales_lag_38: int | None
    sales_lag_39: int | None
    sales_lag_40: int | None
    sales_lag_41: int | None
    sales_lag_42: int | None
    sales_lag_43: int | None
    sales_lag_44: int | None
    sales_lag_45: int | None
    sales_lag_46: int | None
    sales_lag_47: int | None
    sales_lag_48: int | None
    sales_lag_49: int | None
    sales_lag_50: int | None
    sales_lag_51: int | None
    sales_lag_52: int | None
    sales_lag_53: int | None

    @validator(
        "sales_lag_2",
        "sales_lag_3",
        "sales_lag_4",
        "sales_lag_5",
        "sales_lag_6",
        "sales_lag_7",
        "sales_lag_8",
        "sales_lag_9",
        "sales_lag_10",
        "sales_lag_11",
        "sales_lag_12",
        "sales_lag_13",
        "sales_lag_14",
        "sales_lag_15",
        "sales_lag_16",
        "sales_lag_17",
        "sales_lag_18",
        "sales_lag_19",
        "sales_lag_20",
        "sales_lag_21",
        "sales_lag_22",
        "sales_lag_23",
        "sales_lag_24",
        "sales_lag_25",
        "sales_lag_26",
        "sales_lag_27",
        "sales_lag_28",
        "sales_lag_29",
        "sales_lag_30",
        "sales_lag_31",
        "sales_lag_32",
        "sales_lag_33",
        "sales_lag_34",
        "sales_lag_35",
        "sales_lag_36",
        "sales_lag_37",
        "sales_lag_38",
        "sales_lag_39",
        "sales_lag_40",
        "sales_lag_41",
        "sales_lag_42",
        "sales_lag_43",
        "sales_lag_44",
        "sales_lag_45",
        "sales_lag_46",
        "sales_lag_47",
        "sales_lag_48",
        "sales_lag_49",
        "sales_lag_50",
        "sales_lag_51",
        "sales_lag_52",
        "sales_lag_53",
        pre=True
    )
    def validate_sales_lag(cls, value=int | None) -> int | None:
        if value is None:
            return None
        if value >= 0:
            return value
        raise ValueError("invalid value.")


class WeeklySalesData(BaseModel):
    weekly_sales_data: DataFrame

    @classmethod
    def from_days_sales_data(cls, sales_data: SalesData) -> "WeeklySalesData":
        df_sales_data = sales_data.sales_data
        df_sales_data = df_sales_data.with_columns(pl.col("date").dt.year().alias("year"))
        df_sales_data = df_sales_data.with_columns(pl.col("date").dt.month().alias("month"))
        # agg（monthはその週で多いほうの月に合わせる）
        weekly_df = df_sales_data.groupby(
            by=["year", "week_of_year", "store", "item"]
        ).agg(
            [
                pl.col("month").mean(),
                pl.col("item_price").mean(),
                pl.col("sales").sum(),
                pl.col("total_sales_amount").sum(),
            ]
        )
        # cast each col
        weekly_df = weekly_df.with_columns(
            pl.col("month").cast(pl.Int16),
            pl.col("item_price").cast(pl.Int32),
            pl.col("sales").cast(pl.Int32),
            pl.col("total_sales_amount").cast(pl.Int32),
        )
        # sort
        weekly_df = weekly_df.sort(
            by=["year", "month", "week_of_year", "store", "item"]
        )
        # add lag data col
        for n_week in range(2, 54, 1):
            weekly_df = weekly_df.with_columns(
                pl.col("sales")
                .shift(periods=n_week)
                .over(["store", "item"])
                .alias(f"sales_lag_{n_week}")
            )
        # check
        WeeklySalesDataSchema.validate(weekly_df)
        return WeeklySalesData(weekly_sales_data=weekly_df)
    
    class Config:
        arbitrary_types_allowed = True

if __name__ == "__main__":
    sales_data = SalesData.from_csv(path="samples\input\item_sales_records_2017_2021.csv")
    print(sales_data)
    weekly_sales_data = WeeklySalesData.from_days_sales_data(sales_data=sales_data)
    print(weekly_sales_data)