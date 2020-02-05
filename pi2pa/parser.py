#!/usr/bin/env python
# coding: utf-8

# # Código para a extração de dados, criação do esquema e povoamento das relações
# ## Autores: Agnaldo B. Júnior e Pedro H. C. dos Santos
# 
# ### Código para o parse do arquivo:

# In[1]:


def get_line_number(file_path):
    sys.stderr.write("Counting line number of {}".format(file_path))
    f = gzip.open(file_path)
    lines = f.readlines()
    count =0
    
    for line in lines:
        count += 1
    return count

def parse(filename, total):
    IGNORE_FIELDS = ['Total items', 'reviews']
    f = open(filename, 'r')
    lines = f.readlines()
    entry = {}
    categories = []
    reviews = []
    similar_items = []
  
    for line in lines[0:total]:
        colonPos = line.find(':')

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories
            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos+2:]
            entry["id"] = rest[1:-1]
      
        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append({'customer_id': review_info[2], 
                          'rating': int(review_info[4]), 
                          'votes': int(review_info[6]), 
                          'helpful': int(review_info[8]),
                           'date':review_info[0]})

        elif line.startswith("   |"):
            categories.append(line[0:-1].replace(' ',''))

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos+2:]
            if not eName in IGNORE_FIELDS:
                if eName[0] == ' ':
                    eName = eName[2:]
                entry[eName] = rest[0:-1].replace("'","''")

    if reviews:
        entry["reviews"] = reviews
    if categories:
        entry["categories"] = categories
    
    yield entry


# In[2]:


def read_file(file_path = "data/amazon-meta.txt"):
    line_num = sum(1 for line in open(file_path))
    result = []
    for e in parse(file_path, total=line_num):
        if e:
            result.append(e)
    return result


# ### Código para a criação do esquema do banco de dados:

# In[3]:


import psycopg2
def create_schema(user, password, host, port, dbname):
    con = psycopg2.connect( user = user,
                            password = password,
                            host = host,
                            port = port,
                            dbname = dbname)
    cursor = con.cursor()
    cursor.execute('''create table if not exists products
    (
      id integer,
      asin varchar not null
        constraint products_pk
          primary key,
      title varchar,
      "group" varchar,
      salesrank integer
    );''')
    cursor.execute('''
    alter table products
        owner to postgres;
    ''')

    cursor.execute('''create unique index if not exists products_asin_uindex
      on products (asin);
    ''')

    cursor.execute('''create table if not exists similars
    (
      asin_1 varchar not null
        constraint similars_products_asin_fk
          references products,
      asin_2 varchar
    );
    ''')

    cursor.execute('''
    create table if not exists categories
    (
      id integer not null
        constraint categories_pk
          primary key,
      name varchar
    );
    ''')

    cursor.execute('''create table if not exists product_category
    (
      product_asin varchar
        constraint product_category_products_asin_fk
          references products,
      category_id integer
        constraint product_category_categories_id_fk
          references categories
    );
    ''')

    cursor.execute('''create unique index if not exists product_category_product_asin_category_id_uindex
      on product_category (product_asin, category_id);
    ''')

    cursor.execute('''create table if not exists reviews
    (
      id serial not null
        constraint reviews_pk
          primary key,
      date date,
      customer varchar,
      rating integer,
      votes integer,
      helpful integer,
      product_asin varchar
        constraint reviews_products_asin_fk
          references products
    );
    ''')

    cursor.execute('''create unique index if not exists reviews_id_uindex
      on reviews (id);
    ''')

    con.commit()

    cursor.close()
    con.close()


# ### Código para o povoamento do banco de dados

# In[4]:


def populate_database(user,password,host,port,dbname,data):
    result = data
    necessaryKeys = ["id","ASIN","title","group","salesrank","similar","categories","reviews"]

    con = psycopg2.connect( user = user,
                            password = password,
                            host = host,
                            port = port,
                            dbname = dbname)
    cursor = con.cursor()
    cursor.execute('DELETE FROM reviews;DELETE FROM product_category;DELETE FROM categories;DELETE FROM similars;DELETE FROM products;')
    con.commit()
    # -------------------------------------
    for value in result:
        for key in necessaryKeys:
            if key not in list(value.keys()):
                value[key] = "null"
            else:
                if key != 'id' and key != 'salesrank' and key !='similar' and key!='categories' and key != 'reviews':
                    value[key] = "'{}'".format(value[key])
        query = 'INSERT INTO products VALUES ({},{},{},{},{})'.format(int(value['id']),value['ASIN'],value['title'],value['group'],value['salesrank'])
        cursor.execute(query)

        query = 'INSERT INTO similars VALUES '
        similars = value['similar'][1:].split('  ')[1:]
        if similars:
            for similar in similars:
                query = query + '({},\'{}\'),'.format(value['ASIN'],similar)
            query = query[:-1]
            cursor.execute(query)
        if type(value['categories']) == list:    
            for category in value['categories']:
                category = category.split('|')
                for x in category[1:]:
                    query = 'INSERT INTO categories VALUES ' 
                    x = x.split('[')
                    try:
                        query = query + '({},\'{}\') ON CONFLICT DO NOTHING'.format(int(x[1][:-1]),x[0].replace("'","''"))
                    except:
                        query = query + '({},\'{}\') ON CONFLICT DO NOTHING'.format(int(x[2][:-1]),x[0].replace("'","''"))
                    cursor.execute(query)
                    try:
                        query = 'INSERT INTO product_category VALUES ({},{}) ON CONFLICT DO NOTHING'.format(value['ASIN'],int(x[1][:-1]))
                    except:
                        query = 'INSERT INTO product_category VALUES ({},{}) ON CONFLICT DO NOTHING'.format(value['ASIN'],int(x[2][:-1]))
                    cursor.execute(query)
        if type(value['reviews']) == list:
            for review in value['reviews']:
                query = '''INSERT INTO 
                        reviews(date, customer, rating, votes, helpful, product_asin)
                        VALUES (\'{}\',\'{}\',{},{},{},{})'''.format(review['date'],review['customer_id'],review['rating'],review['votes'],review['helpful'],value['ASIN'])
                cursor.execute(query)
        con.commit()
    # -------------------------------------

    cursor.close()
    con.close()


# In[5]:


print("Insira os parâmetros da conexão: Usuário, senha, endereço do servidor, porta, nome da database")
print("Porta padrão é 5432")
user = input("Usuário:")
password = input("Senha:")
host = input("Endereço:")
port = input("Porta:")
dbname = input("Nome da database:")
filePath = input("Insira o path para o arquivo de entrada (Ex: 'data/amazon-meta.txt'):")
# user = "postgres"
# password = "postgres"
# host = "localhost"
# port = "5432"
# dbname = "teste"
data = read_file(filePath)
create_schema(user,password,host,port,dbname)
populate_database(user,password,host,port,dbname,data)


# In[ ]:




