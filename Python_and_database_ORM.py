import json

import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

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

def get_book_sale(writer=input("Введите имя издателя: ")):
    selected = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
               .join(Publisher).join(Stock).join(Shop).join(Sale).filter(Publisher.name.like(writer))
    for s in selected.all():
        print(f'{s[0]} | {s[1]} | {str(s[2])} | {str(s[3])}')


get_book_sale()

session.close()