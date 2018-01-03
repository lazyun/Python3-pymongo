import pymongo
import urllib.parse


mongo_client = None

# user passwd 为空或字符串
mongo_cfg = {
    "host": "127.0.0.1",
    "port": 27017,
    "user": "",
    "passwd": ""
}

def connect_mongo():
    # 链接 mongodb
    global mongo_client
    host = urllib.parse.quote_plus(mongo_cfg['host'])
    port = mongo_cfg['port']
    user = urllib.parse.quote_plus(mongo_cfg['user'])
    if not user:
        mongo_client = pymongo.MongoClient()
        return

    pw = urllib.parse.quote_plus(mongo_cfg['passwd'])
    mongo_url = 'mongo://{}:{}@{}:{}'.format(user, pw, host, port)
    mongo_client = pymongo.MongoClient(mongo_url)


def create_col(db_name, col_name, autoindex=1):
    # 创建集合
    mongo_db = mongo_client[db_name]
    mongo_db.create_collection(col_name, autoIndexId=autoindex)


def create_col_index(db_name, col_name, index_colunm):
    # 创建独立索引
    mongo_db = mongo_client[db_name]
    mongo_db[col_name].create_index( [ (index_colunm, pymongo.ASCENDING) ], background = 1, unique = 1)


def create_col_union_index(db_name, col_name, index_colunm_1, index_colunm_2):
    # 创建联合索引
    mongo_db = mongo_client[db_name]
    # create_index( [ (index_colunm, 'hashed', pymongo.ASCENDING) ] )
    mongo_db[col_name].create_index( [ (index_colunm_1, pymongo.ASCENDING), (index_colunm_2, pymongo.DESCENDING) ], background = 1, unique = 1)


def create_col_indexs(db_name, col_name, index_1, index_2):
    # 创建多个单独索引
    mongo_db = mongo_client[db_name]
    # 联合索引
    # index_12 = pymongo.IndexModel( [ ('qwer', 'hashed', pymongo.ASCENDING), ('rewq', pymongo.ASCENDING) ], name=[] , background = 1)
    index_m_1 = pymongo.IndexModel( [ (index_1, pymongo.ASCENDING) ], background = 1)
    index_m_2 = pymongo.IndexModel( [ (index_2, pymongo.ASCENDING) ], background = 1)
    mongo_db[col_name].create_indexes( [index_m_1, index_m_2] )



def create_sharding(sharding_col_db, col_name, sharding_colunm, ishashed=False):
    '''
    为一个集合进行分片、集合所在的数据库需要有分片权限、分片的 key 需要有对应的索引
    :param sharding_col_db: 分片集合所在的数据库
    :param col_name: 分片集合的名字
    :param sharding_colunm: 分片的 key
    :param ishashed: 是否为 hash 分片
    :return: {'collectionsharded': 'sharding_col_db.col_name', 'ok': 1.0}
    '''
    admin_db = mongo_client['admin']
    abs_path = '{}.{}'.format(sharding_col_db, col_name)
    sharding_type = 1 if not ishashed else 'hashed'
    return admin_db.command('shardCollection', abs_path, key = {sharding_colunm: sharding_type})