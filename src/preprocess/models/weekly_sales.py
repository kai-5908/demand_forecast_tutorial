from datetime import datetime, date
from pathlib import Path

import patito
from patito import Field
from pydantic import BaseModel
import polars as pl
from polars import DataFrame

from .day_of_week import DAYS_OF_WEEK
from .stores import STORES
from .items import ITEMS
from .sales import SalesData


class WeeklySalesDataSchema(patito.Model):
    date: datetime
    year: int
    week_of_year: int = patito.Field(ge=1, le=54)
    month: int = patito.Field(ge=1, le=12)
    store: STORES
    item: ITEMS
    item_price: int = patito.Field(ge=0)
    sales: int = patito.Field(ge=0)
    total_sales_amount: int = patito.Field(ge=0)
    sales_lag_2: float | None = patito.Field(ge=0)
    sales_lag_3: float | None = patito.Field(ge=0)
    sales_lag_4: float | None = patito.Field(ge=0)
    sales_lag_5: float | None = patito.Field(ge=0)
    sales_lag_6: float | None = patito.Field(ge=0)
    sales_lag_7: float | None = patito.Field(ge=0)
    sales_lag_8: float | None = patito.Field(ge=0)
    sales_lag_9: float | None = patito.Field(ge=0)
    sales_lag_10: float | None = patito.Field(ge=0)
    sales_lag_11: float | None = patito.Field(ge=0)
    sales_lag_12: float | None = patito.Field(ge=0)
    sales_lag_13: float | None = patito.Field(ge=0)
    sales_lag_14: float | None = patito.Field(ge=0)
    sales_lag_15: float | None = patito.Field(ge=0)
    sales_lag_16: float | None = patito.Field(ge=0)
    sales_lag_17: float | None = patito.Field(ge=0)
    sales_lag_18: float | None = patito.Field(ge=0)
    sales_lag_19: float | None = patito.Field(ge=0)
    sales_lag_20: float | None = patito.Field(ge=0)
    sales_lag_21: float | None = patito.Field(ge=0)
    sales_lag_22: float | None = patito.Field(ge=0)
    sales_lag_23: float | None = patito.Field(ge=0)
    sales_lag_24: float | None = patito.Field(ge=0)
    sales_lag_25: float | None = patito.Field(ge=0)
    sales_lag_26: float | None = patito.Field(ge=0)
    sales_lag_27: float | None = patito.Field(ge=0)
    sales_lag_28: float | None = patito.Field(ge=0)
    sales_lag_29: float | None = patito.Field(ge=0)
    sales_lag_30: float | None = patito.Field(ge=0)
    sales_lag_31: float | None = patito.Field(ge=0)
    sales_lag_32: float | None = patito.Field(ge=0)
    sales_lag_33: float | None = patito.Field(ge=0)
    sales_lag_34: float | None = patito.Field(ge=0)
    sales_lag_35: float | None = patito.Field(ge=0)
    sales_lag_36: float | None = patito.Field(ge=0)
    sales_lag_37: float | None = patito.Field(ge=0)
    sales_lag_38: float | None = patito.Field(ge=0)
    sales_lag_39: float | None = patito.Field(ge=0)
    sales_lag_40: float | None = patito.Field(ge=0)
    sales_lag_41: float | None = patito.Field(ge=0)
    sales_lag_42: float | None = patito.Field(ge=0)
    sales_lag_43: float | None = patito.Field(ge=0)
    sales_lag_44: float | None = patito.Field(ge=0)
    sales_lag_45: float | None = patito.Field(ge=0)
    sales_lag_46: float | None = patito.Field(ge=0)
    sales_lag_47: float | None = patito.Field(ge=0)
    sales_lag_48: float | None = patito.Field(ge=0)
    sales_lag_49: float | None = patito.Field(ge=0)
    sales_lag_50: float | None = patito.Field(ge=0)
    sales_lag_51: float | None = patito.Field(ge=0)
    sales_lag_52: float | None = patito.Field(ge=0)


class WeeklySalesData(BaseModel):
    weekly_sales_data: DataFrame

    @classmethod
    def from_days_sales_data(cls, sales_data: SalesData) -> "WeeklySalesData":
        df_sales_data = sales_data.sales_data
        df_sales_data["year"] = df_sales_data["date"].dt.year
        df_sales_data["month"] = df_sales_data["date"].dt.month
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
        weekly_df = weekly_df.select(
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
            weekly_df.with_columns(
                pl.col("sales")
                .shift(periods=n_week)
                .over(["store", "item"])
                .alias(f"sales_lag_{n_week}")
            )
        # check
        WeeklySalesDataSchema.validate(weekly_df)
        return WeeklySalesData(weekly_sales_data=weekly_df)
