from io import BytesIO
import pandas as pd

COLUMN_RENAME = {
        "Код Инструмента": "exchange_product_id",
        "Наименование Инструмента": "exchange_product_name",
        "Базис поставки": "delivery_basis_name",
        "Объем Договоров в единицах измерения": "volume",
        "Обьем Договоров, руб.": "total",
        "Количество Договоров, шт.": "count",
}
COLUMN_CONVERT_TYPES = ["volume", "total", "count"]
EMPTY_VALUE = 0
FIRST_INDEX = 0
LAST_INDEX = -1
OIL_ID_SLICE = slice(0, 4)
DELIVERY_BASIS_ID_SLICE = slice(4, 7)
DELIVERY_TYPE_ID_SLICE = slice(-1, None)
DATE_FORMAT = "%d.%m.%Y"
DATE_COLUMN_NAME = "date"
DATE_ROW_INDEX = 2
DATE_COLUMN_INDEX = 1
TABLE_TAG_COLUMN_INDEX = 1
TABLE_TAG = "Единица измерения: Метрическая тонна"
HEADER_OFFSET = 1
COLUMS_TO_SELECT = [1, 2, 3, 4, 5, -1]
ROWS_TO_DROP = [0, 1]
EXCLUDE_SYMBOL = "-"


def format_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_RENAME)
    df[DATE_COLUMN_NAME] = pd.to_datetime(df[DATE_COLUMN_NAME], format=DATE_FORMAT)
    for col in COLUMN_CONVERT_TYPES:
        df[col] = df[col].fillna(EMPTY_VALUE).astype(int)
    df["oil_id"] = df["exchange_product_id"].str[OIL_ID_SLICE]
    df["delivery_basis_id"] = df["exchange_product_id"].str[DELIVERY_BASIS_ID_SLICE]
    df["delivery_type_id"] = df["exchange_product_id"].str[DELIVERY_TYPE_ID_SLICE]
    return df


def parse_data(file_data: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_data))
    date = str(df.iloc[DATE_ROW_INDEX, DATE_COLUMN_INDEX]).rsplit(" ", maxsplit=1)[LAST_INDEX]
    index_of_header = df[df.iloc[:, TABLE_TAG_COLUMN_INDEX] == TABLE_TAG].index[FIRST_INDEX]
    df = df.iloc[index_of_header + HEADER_OFFSET:, COLUMS_TO_SELECT].reset_index(drop=True)
    df.columns = pd.Index(df.iloc[FIRST_INDEX].str.replace("\n", " "))
    df = df.drop(ROWS_TO_DROP)
    df = df[df.iloc[:, LAST_INDEX] != EXCLUDE_SYMBOL].dropna().reset_index(drop=True)
    df[DATE_COLUMN_NAME] = date
    return format_df_for_db(df)
