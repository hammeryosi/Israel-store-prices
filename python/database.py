import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, Sequence
import xml.etree.ElementTree as ET
import os

def connect(user, password, db, host='localhost', port=5432):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta

def generate_tables_in_db():
    con, meta = connect('alongat', '123456', 'israeli_prices')
    chains = Table('chains', meta,
                   Column('id', String, nullable=False, primary_key=True),
                   Column('name', String, nullable=False),
                   extend_existing=True)
    stores = Table('stores', meta,
                   Column('seq_id', Integer, Sequence('store_id_seq', metadata=meta), primary_key=True),
                   Column('id', Integer, nullable=False),
                   Column('name', String, nullable=False),
                   Column('chain', ForeignKey('chains.id')),
                   Column('address', String),
                   extend_existing=True)
    items = Table('items', meta,
                  Column('id', Integer),
                  Column('seq_id', Integer, Sequence('item_id_seq', metadata=meta), primary_key=True),
                  Column('item_code', String),
                  Column('store', ForeignKey('stores.seq_id')),
                  Column('name', String, nullable=False),
                  Column('description', String, nullable=False),
                  Column('type', Integer, nullable=False),
                  Column('price', sqlalchemy.types.FLOAT, nullable=False),
                  Column('update_at', sqlalchemy.types.DateTime, nullable=False),
                  Column('manufacturer_name', String),
                  Column('quantity', sqlalchemy.types.Float),
                  Column('unit_of_measure', String),
                  extend_existing=True)
    meta.create_all(con)
    return con, meta, chains, stores, items

def import_stores_to_db(path):
    con, meta, chains, stores, items = generate_tables_in_db()
    for filename in os.listdir(path):
        if not filename.startswith('Stores'):
            continue
        if not filename.endswith('.xml') and not filename.endswith('.gz'):
            continue
        tree = ET.parse(path+'/'+filename)
        root = tree.getroot()
        chain_name = root.find('ChainName').text
        chain_id = int(root.find('ChainId').text)
        clause = chains.insert().values(name=chain_name, id=chain_id)
        con.execute(clause)
        list_of_stores = []
        for item in root.findall('.//SubChains/SubChain/Stores/Store'):
            store = {'id': int(item.find('StoreId').text),
                     'name': item.find('StoreName').text,
                     'address': item.find('Address').text + ' ' + item.find('City').text,
                     'chain': chain_id}
            list_of_stores.append(store)
        con.execute(meta.tables['stores'].insert(), list_of_stores)

def import_items_to_db(path):
    con, meta, chains, stores, items = generate_tables_in_db()
    for filename in os.listdir(path):
        if not filename.startswith('Price'):
            continue
        if not filename.endswith('.xml') and not filename.endswith('.gz'):
            continue
        tree = ET.parse(path+'/'+filename)
        root = tree.getroot()
        store_id = root.find('StoreId').text
        chain_id = root.find('ChainId').text
        res = con.execute(stores.select().where(stores.c.id == store_id).where(stores.c.chain == chain_id)).fetchall()
        if len(res) > 1:
            raise UserWarning('More than one store found', store_id, chain_id)
        if len(res) == 0:
            print('No Store found!', store_id, chain_id)
            continue

        list_of_items = []
        for item in root.findall('.//Items/Item'):
            store = {'id': int(item.find('ItemId').text),
                     'store': res[0].seq_id,
                     'name': item.find('ItemName').text,
                     'item_code': item.find('ItemCode').text,
                     'description': item.find('ManufacturerItemDescription').text,
                     'type': int(item.find('ItemType').text),
                     'price': float(item.find('ItemPrice').text),
                     'update_at': item.find('PriceUpdateDate').text,
                     'manufacturer_name': item.find('ManufacturerName').text,
                     'quantity': float(item.find('Quantity').text),
                     'unit_of_measure': item.find('UnitOfMeasure').text}
            list_of_items.append(store)
        con.execute(meta.tables['items'].insert(), list_of_items)
