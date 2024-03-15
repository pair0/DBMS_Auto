from cassandra.auth import PlainTextAuthProvider  # type: ignore
from cassandra.cluster import Cluster  # type: ignore


class CassandraInspector:
    def __init__(
        self, server_ip, server_port, server_username, server_password, server_keyspace
    ):
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_keyspace = server_keyspace

        auth_provider = PlainTextAuthProvider(
            username=server_username, password=server_password
        )
        self.cluster = Cluster(
            [server_ip], port=server_port, auth_provider=auth_provider
        )
        self.session = self.cluster.connect(server_keyspace)

    # DB-01, High
    def DB_01(self):
        answer = [None] * 2
        answer[0] = "CDB-01"

        try:
            auth_provider_test = PlainTextAuthProvider(
                username="cassandra", password="cassandra"
            )
            cluster_test = Cluster(
                [self.server_ip],
                port=self.server_port,
                auth_provider=auth_provider_test,
            )
            session_test = cluster_test.connect(self.server_keyspace)
            answer[1] = "기본 계정의 디폴트 패스워드를 변경하지 않고 운영하고 있음"
            return False, answer

        except Exception as e:
            if "AuthenticationFailed" in str(e):
                answer[1] = "기본 계정의 디폴트 패스워드를 변경하여 운영하고 있음"
                return True, answer

    # DB-02, High
    def DB_02(self):
        answer = [None] * 2
        answer[0] = "CDB-02"

        out = self.session.execute(
            "select * from system_views.settings where name = 'authenticator';"
        )[0].value

        if out == "PasswordAuthenticator":
            answer[1] = (
                "cassandra.yaml 파일 안에 authenticator 항목이 PasswordAuthenticator로 설정되어 운영되고 있음"
            )
            return True, answer
        else:  # AllowAllAuthenticator or No Setting
            answer[1] = (
                "cassandra.yaml 파일 안에 authenticator 항목이 설정되어 있지 않거나 AllowAllAuthenticator로 설정되어 운영되고 있음"
            )
            return False, answer

    def close_connections(self):
        self.cluster.shutdown()
