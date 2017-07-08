import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, Sequence
import xml.etree.ElementTree as ET
import os
import gzip

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

def to_parseable(tree_string):
    return ET.fromstring(tree_string.lower())

def import_stores_to_db(path):
    con, meta, chains, stores, items = generate_tables_in_db()
    for filename in os.listdir(path):
        if not filename.startswith('Stores'):
            continue
        if filename.endswith('.xml'):
            file = ET.tostring(ET.parse(path + '/' + filename).getroot())
        elif filename.endswith('.gz'):
            file = gzip.GzipFile(path + '/' + filename).read()
        else:
            continue
        root = to_parseable(file)
        chain_name = root.find('.//chainname')
        if chain_name == None:
            chain_name = root.find('.//stores/store/chainname')
        chain_name = chain_name.text
        chain_id = int(root.find('.//chainid').text)
        clause = chains.insert().values(name=chain_name, id=chain_id)
        con.execute(clause)
        list_of_stores = []
        items = root.findall('.//subchains/subchain/stores/store')
        if len(items) == 0:
            items = root.findall('.//stores/store')
        for item in items:
            address = item.find('.//address').text
            city = item.find('.//city').text
            store = {'id': int(item.find('.//storeid').text),
                     'name': item.find('.//storename').text,
                     'address': ('' if address is None else address) + ' ' + '' if city is None else city,
                     'chain': chain_id}
            list_of_stores.append(store)
        con.execute(meta.tables['stores'].insert(), list_of_stores)

def import_items_to_db(path):
    con, meta, chains, stores, items = generate_tables_in_db()
    for filename in os.listdir(path):
        if not filename.startswith('PriceFull'):
            continue
        if filename.endswith('.xml'):
            file = path + '/' + filename
        elif filename.endswith('.gz'):
            file = gzip.GzipFile(path + '/' + filename)
        else:
            continue
        root = to_parseable(file.read())
        store_id = root.find('.//storeid').text
        chain_id = root.find('.//chainid').text
        res = con.execute(stores.select().where(stores.c.id == store_id).where(stores.c.chain == chain_id)).fetchall()
        if len(res) > 1:
            raise UserWarning('More than one store found', store_id, chain_id)
        if len(res) == 0:
            print('No Store found!', store_id, chain_id)
            continue

        list_of_items = []
        for item in root.findall('.//items/item'):
            id = item.find('itemid')
            store = {'id': None if id is None else int(id.text),
                     'store': res[0].seq_id,
                     'name': item.find('itemname').text,
                     'item_code': item.find('itemcode').text,
                     'description': item.find('manufactureritemdescription').text,
                     'type': int(item.find('itemtype').text),
                     'price': float(item.find('itemprice').text),
                     'update_at': item.find('priceupdatedate').text,
                     'manufacturer_name': item.find('manufacturername').text,
                     'quantity': float(item.find('quantity').text),
                     'unit_of_measure': item.find('unitofmeasure').text}
            list_of_items.append(store)
        con.execute(meta.tables['items'].insert(), list_of_items)
