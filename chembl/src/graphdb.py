from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

HOST = 'localhost'
PORT = 9669
USER = 'root'
PASSWORD = 'nebula'

class GraphDB:
    def __init__(self):
        config = Config()
        config.max_connection_pool_size = 10

        self.pool = ConnectionPool()
        self.pool.init([(HOST, PORT)], config)

        self.session = self.pool.get_session(USER, PASSWORD)

        self.session.execute('CREATE SPACE IF NOT EXISTS chembl(partition_num=10, replica_factor=1, vid_type=fixed_string(64));')
        self.session.execute('USE chembl')
    
    
    def create_tag(self, tag_name, properties: dict[str, str]):
        properties_str = ''
        for key, value in properties.items():
            properties_str += f'{key} {value},'
        properties = properties_str[:-1]

        self.session.execute(f'CREATE TAG IF NOT EXISTS {tag_name} ({properties})')


    def create_edge(self, edge_name, properties: dict[str, str]):
        properties_str = ''
        for key, value in properties.items():
            properties_str += f'{key} {value},'
        properties = properties_str[:-1]

        self.session.execute(f'CREATE EDGE IF NOT EXISTS {edge_name} ({properties})')


    def insert_vertex(self, tag_name, vertex_id, properties: dict[str, str]):
        columns_str = ''
        values_str = ''
        for key, value in properties.items():
            columns_str += f'{key},'
            values_str += f'"{value}",'
        columns = columns_str[:-1]
        values = values_str[:-1]

        self.session.execute(f'INSERT VERTEX IF NOT EXISTS {tag_name} ({columns}) VALUES "{vertex_id}" : ({values})')


    def insert_edge(self, edge_name, from_vertex, to_vertex, properties: dict[str, str]):
        columns_str = ''
        values_str = ''
        for key, value in properties.items():
            columns_str += f'{key},'
            values_str += f'"{value}",'
        columns = columns_str[:-1]
        values = values_str[:-1]

        self.session.execute(f'INSERT EDGE IF NOT EXISTS {edge_name} ({columns}) VALUES "{from_vertex}" -> "{to_vertex}" : ({values})')


    def show_tags(self) -> list[str]:
        result = self.session.execute('SHOW TAGS')
        result_in_list = [str(value).replace('"', '') for value in list(result)]
        return result_in_list
    

    def show_edges(self) -> list[str]:
        result = self.session.execute('SHOW EDGES')
        result_in_list = [str(value).replace('"', '') for value in list(result)]
        return result_in_list


    def close(self):
        self.session.release()
        self.pool.close()