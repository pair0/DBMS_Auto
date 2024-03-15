import redis
from datetime import datetime


class RedisInspector:
    def __init__(self, server_ip, server_port, server_username, server_password):
        self.redis_client = redis.StrictRedis(
            host=server_ip,
            port=server_port,
            db=0,
            password=server_password,
            max_connections=4,
        )

    # DB-01, High
    def DB_01(self):
        answer = [None] * 2
        answer[0] = "RDB-01"

        config = self.redis_client.execute_command("config", "get", "requirepass")
        if len(config) == 0:
            out = ""
        else:
            out = str(config[1], "utf-8")

        if len(out) == 0 or out == "foobared":
            answer[1] = "인증패스워드가 설정되어 있지 않음"
            return False, answer
        else:
            answer[1] = "인증패스워드가 설정되어 운영되고 있음"
            return True, answer

    # DB-02, High
    def DB_02(self):
        answer = [None] * 2
        answer[0] = "RDB-02"
        user_list = list()

        # 1. user 조회
        for user in self.redis_client.execute_command("ACL", "LIST"):
            user_list.append(str(user, "utf-8"))

        if len(user_list) != 0:
            answer[1] = f"user list : {', '.join(user_list)}"
            return "Survey", answer
        else:
            answer[1] = "계정 정보가 존재하지 않습니다."
            return False, answer

    def close_connections(self):
        self.redis_client.close()
