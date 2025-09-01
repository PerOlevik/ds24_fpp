import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import sys
from pathlib import Path
import logging
from util import setup_logger, normalize_sales

#  Setup logging 
logger = logging.getLogger("store_sales")
logger.setLevel(logging.INFO)
logger.addHandler(setup_logger())

#  Setup databasen
DB_PATH = Path("data/mydb.sqlite")
ENGINE = create_engine(f"sqlite:///{DB_PATH}")


#  Main function ETL
if __name__ == "__main__":
    def update_table():
        try:
        # Läs in CSV (semicolon-separerad)
            df = pd.read_csv("data/input.csv", sep=";", skip_blank_lines=True, parse_dates=["date"])
            logger.info(f"Läser in CSV.") 

            # Letar efter missing value i store och tar bort sådana rader
            missing_store = df["store"].isna().sum()
            if missing_store > 0:
                logger.warning("CSV contained %d rows with missing store. These rows will be ignored.", missing_store)
                df = df[df["store"].notna()]          


        # Skapa SQL-tabellen om den inte finns
            with ENGINE.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS my_table (
                        date TEXT,
                        store INTEGER,
                        sales REAL
                    )
                """))

        # Loop per butik
            for store_id in df["store"].unique():
                store_df = df[df["store"] == store_id]
                logger.info(f"Checking store {store_id}.")
            
                # letar efter missing value i date och tar bort sådana rader
                missing_date = store_df["date"].isna().sum()
                if missing_date > 0:
                    logger.warning("CSV contained %d rows with missing date for id %s. These rows will be ignored.", missing_date, store_id)
                    store_df = store_df[store_df["date"].notna()]                  

            # Hämta senaste datumet i databasen för den här butiken
                with ENGINE.connect() as conn:
                    result = conn.execute(
                        text("SELECT MAX(date) FROM my_table WHERE store = :s"),
                        {"s": int(store_id)}
                    ).scalar()

                # Om det inte finns något i databasen än → ta allt
                if result is None:
                    new_data = store_df.copy()
                else:
                # Konvertera result till pandas Timestamp för jämförelse
                    latest_date = pd.to_datetime(result)
                    new_data = store_df[store_df["date"] > latest_date].copy()

               
                # Konvertera sales till enhetligt format
                new_data["sales"] = new_data["sales"].apply(normalize_sales)
     
                if new_data["sales"] is None:
                    logger.warning("Sales conversion failed.")
                                 
                if not new_data.empty:
                    new_data.to_sql("my_table", con=ENGINE, if_exists="append", index=False)
                    logger.info("Store %s: lade in %d nya rader.", store_id, len(new_data))

                else:
                    logger.info("Store %s: inga nya rader att lägga in.", store_id)


        except FileNotFoundError as e:
            logger.error("CSV-fil saknas: %s", e)


        except Exception as e:
            logger.error("Okänt fel: %s", e, exc_info=True)



    update_table()

