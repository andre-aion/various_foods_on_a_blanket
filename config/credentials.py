credentials = {
    'localhost': {

        'postgres': {
            'user': 'admin',
            'host': '127.0.0.1',
            'db': 'amdattds',
            'password': 'password',
            'port': "5432"
        },
        'clickhouse': {
            'user': 'admin',
            'host': '127.0.0.1',
            'db': 'amdattds',
            'password': 'password',
            'port': "5432"
        },
        'redis':{
            'host': '127.0.0.1',
        },
        'websocket': 'localhost',
    },
    'dev': {
            'clickhouse' : {
                'user':'admin',
                'host':'192.168.99.100',
                'db':'amdattds',
                'password': 'password',
                'port':"8123"
            },
            'postgres' : {
                        'user':'admin',
                        'host':'192.168.99.100',
                        'db':'amdattds',
                        'password': 'password',
                        'port':"5432"
                    },
            'redis': {
                'host': '192.168.99.100',
            },

            'websocket': '192.168.99.100',
        },
}