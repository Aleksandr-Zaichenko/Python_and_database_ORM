import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale

import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

DSN = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Задание 3, заполните БД тестовыми данными.

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

# Задание 2, составить запрос выборки магазинов, продающих целевого издателя.

def get_shops(writer):
    selected = session.query(
        Book.title, Shop.name, Sale.price, Sale.date_sale
    ).select_from(Shop).\
        join(Stock).\
        join(Book).\
        join(Publisher).\
        join(Sale)
    if writer.isdigit():
        result = selected.filter(Publisher.id == writer).all()
    else:
        result = selected.filter(Publisher.name == writer).all()
    for title, name, price, date_sale in result:
        print(f"{title: <40} | {name: <10} | {price: <8} | {date_sale.strftime('%d-%m-%Y')}")

if __name__ == '__main__':
    writer = input("Введите имя или идентификатор издателя: ")
    get_shops(writer)

session.close()