from pymongo import MongoClient
from collections import defaultdict


class MongodbInspector:
    def __init__(self, server_ip, server_port, server_username, server_password):
        self.mongo_client = MongoClient(
            host=server_ip,
            port=server_port,
            username=server_username,
            password=server_password,
        )

    # DB-01, High
    def DB_01(self):
        answer = [None] * 2
        answer[0] = "DB-01"
        admin_user_list = list()

        for admin_user in self.mongo_client.admin.system.users.find(
            {"roles": {"$elemMatch": {"role": "root"}}}
        ):
            admin_user_list.append(admin_user["user"])

        if len(admin_user_list) > 0:
            answer[1] = ", ".join(admin_user_list)
            return True, answer
        else:
            answer[1] = "관리자 계정이 생성되어있지 않음"
            return False, answer

    # DB-02, High
    def DB_02(self):
        answer = [None] * 2
        answer[0] = "DB-02"
        out = defaultdict(list)

        # 2. database & collections 조회
        for database in self.mongo_client.list_database_names():
            out[database] = self.mongo_client[database].list_collection_names()

        if len(out) != 0:
            out_str = ""
            for database, collection in out.items():
                out_str += f"{database} : {', '.join(collection)}\n"
            answer[1] = out_str
            return "Survey", answer
        else:
            answer[1] = "데이터베이스 및 collections이 존재하지 않습니다."
            return False, answer

    def close_connections(self):
        self.mongo_client.close()
