"""Application for entering data from a CSV file into an SQL database."""


from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import csv
import sys

import pyodbc

@dataclass
class Config:
    csv_path: Path


class Manufacturer(Enum):
    A = 1
    G = 2
    K = 3
    N = 4
    P = 5
    Q = 6
    R = 7


class Cereal_Type(Enum):
    C = 1
    H = 2


@dataclass
class Entry:
    name: str
    manufacturer: str
    cereal_type: str
    calories: int
    protein: int
    fat: int
    sodium: int
    fiber: float
    carbo: float
    sugars: int
    potass: int
    vitamins: int
    shelf: int
    weight: float
    cups: float
    rating: float


def read_args() -> Config:
    csv_path = Path(sys.argv[1])
    return Config(csv_path)

def read_csv(config: Config) -> list[Entry]:
    entries: list[Entry] = []
    with open(config.csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            # Logic for doubling ' so that they're added correctly to SQL.
            name = row["name"]
            index = name.find("'")
            while index > -1:
                name = f"{name[:index]}'{name[index:]}"
                index += 2
                if name[index:].find("'") != -1:
                    index += name[index:].find("'")
                else:
                    index = -1

            entry = Entry(name,
                          row["mfr"],
                          row["type"],
                          int(row["calories"]),
                          int(row["protein"]),
                          int(row["fat"]),
                          int(row["sodium"]),
                          float(row["fiber"]),
                          float(row["carbo"]),
                          int(row["sugars"]),
                          int(row["potass"]),
                          int(row["vitamins"]),
                          int(row["shelf"]),
                          float(row["weight"]),
                          float(row["cups"]),
                          float(row["rating"]))
            entries.append(entry)
    return entries


def update_database(config: Config):
    entries = read_csv(config)

    table = "Cereals"
    sql_query = (f"INSERT INTO {table} "
                 "(Name, Manufacturer, CerealType, Calories, Protein, Fat,"
                 " Sodium, Fiber, Carbohydrates, Sugars, Potassium, Vitamins,"
                 " Shelf, Weight, Cups, Rating) VALUES ")

    for entry in entries:
        sql_query += (f"('{entry.name}', '{entry.manufacturer}', "
                      f"'{entry.cereal_type}', {entry.calories}, "
                      f"{entry.protein}, {entry.fat}, {entry.sodium}, "
                      f"{entry.fiber}, {entry.carbo}, {entry.sugars}, "
                      f"{entry.potass}, {entry.vitamins}, {entry.shelf}, "
                      f"{entry.weight}, {entry.cups}, {entry.rating}), ")

    sql_query = sql_query[:-2]
    sql_query += ";"

    connection_string = ("DRIVER={ODBC Driver 18 for SQL Server};"
                         "SERVER={(localdb)\\MSSQLLocalDB};"
                         "DATABASE=CerealDatabase")
    conn = pyodbc.connect(connection_string)
    conn.execute(sql_query)
    conn.commit()
    conn.close()

def main():
    config = read_args()

    update_database(config)

if __name__ == "__main__":
    main()
