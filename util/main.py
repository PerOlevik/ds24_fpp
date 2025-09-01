import logging
import pandas as pd
import re

logger = logging.getLogger("store_sales.util")
logger.setLevel(logging.DEBUG)

def setup_logger(logfile="logs.log"):
    """Set up logger.
    Create a FileHandler, add a Formatter and return the FileHandler.
    """
    fh = logging.FileHandler(f"logs/{logfile}")
    formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s] %(message)s")

    fh.setFormatter(formatter)
    return fh

# Funktion för tvättning av sales-datan.
# Multiplikatorer för kortformer
UNIT_FACTORS = {
    "": 1,
    "kr": 1,
    "sek": 1,
    "k": 1_000,
    "t": 1_000,     # ibland används 't' för tusen
    "tkr": 1_000,
    "ksek": 1_000,
    "tsek": 1_000,
    "m": 1_000_000,
    "milj": 1_000_000,
    "msek": 1_000_000,
}

def normalize_sales(value) -> float | None:
    if pd.isna(value):
        logger.warning("CSV contained a None value for sales")
        return None

    s = str(value).lower().strip().replace(" ", "")  # ta bort mellanslag

    # Regex för siffror + enhet
    match = re.match(r"^([\d.,]+)([a-z]*)$", s)
    if not match:
        logger.error(f"Could not interpret sales value: {value}")
        return None

    number, unit = match.groups()

    # Normalisera decimaltecken
    number = number.replace(",", ".")
    try:
        num = float(number)
    except ValueError:
        logger.error(f"Could not convert number: {value}")
        return None

    factor = UNIT_FACTORS.get(unit)
    if factor is None:
        logger.error(f"Unknown unit '{unit}' in value: {value}")
        return None

    sales_sek = num * factor
    logger.info(f"{value} transformed to {sales_sek}")
    return sales_sek