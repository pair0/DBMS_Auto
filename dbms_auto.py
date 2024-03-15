import argparse
import json
import logging
import multiprocessing
import os
from datetime import datetime
from typing import List, Optional

import pandas as pd  # type: ignore
import pymysql  # type: ignore
from dbms_cassandra_query import CassandraInspector
from dbms_mongodb_query import MongodbInspector
from dbms_redis_query import RedisInspector
from dotenv import load_dotenv

# Configure logging to write errors to a file
log_file = "error_log.txt"
logging.basicConfig(filename=log_file, level=logging.ERROR)


def getenv(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key, default)


# Result 저장 DB connect
def conn_sql(ip, username, password, db, charset):  # 접속
    try:
        conn = pymysql.connect(
            host=ip, user=username, password=password, db=db, charset=charset
        )
        cur = conn.cursor()  # 커서생성
        return conn, cur
    except Exception as e:
        print(f"Error connecting to the database {ip}: {str(e)}")
        logging.error(f"Error connecting to the database {ip}: {str(e)}")
        return None, None


def save_results(results, args):
    if args.excell:
        save_to_excel(results, args.file)
    elif args.csv:
        save_to_csv(results, args.file)
    elif args.databases:
        if args.file:
            print("데이터베이스 저장은 -f 금지")
        else:
            save_to_database(results)


def save_to_database(results):
    db_servers_str: Optional[str] = getenv("DB_SERVERS")

    if db_servers_str is not None:
        try:
            db_servers: dict = json.loads(db_servers_str)
            required_keys = ["ip", "user", "passwd", "table"]

            for key in required_keys:
                if key not in db_servers:
                    raise ValueError(
                        f"{key} is missing in DB_SERVERS environment variable."
                    )

            conn, cur = conn_sql(
                db_servers["ip"],
                db_servers["user"],
                db_servers["passwd"],
                db_servers["table"],
                "utf8",
            )

            if conn is not None or cur is not None:
                save_to_database_internal(conn, cur, db_servers["ip"], results)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in DB_SERVERS: {e}")
            logging.error(f"Error decoding JSON in DB_SERVERS: {e}")
        except ValueError as e:
            print(f"Error in DB_SERVERS configuration: {e}")
            logging.error(f"Error in DB_SERVERS configuration: {e}")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            logging.error(f"Error connecting to the database: {e}")
    else:
        print("DB_SERVERS environment variable is not set.")
        logging.error("DB_SERVERS environment variable is not set.")


def save_to_database_internal(conn, cur, server_ip, results):
    try:
        sql = "INSERT INTO db_insp_result (RUN_DATE, IP, RUN_TIME, INSP_CODE, INSP_RES_CODE, INSP_RES_DATA) VALUES (%s, %s, %s, %s, %s, %s)"
        for _, row in results.iterrows():
            cur.execute(
                sql,
                (
                    row["RUN_DATE"],
                    row["IP"],
                    row["RUN_TIME"],
                    row["INSP_CODE"],
                    row["INSP_RES_CODE"],
                    row["INSP_RES_DATA"],
                ),
            )

        conn.commit()
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
        logging.error(
            f"Error inserting data into the database {server_ip}: {str(e)}"
        )  # 에러 값 로그 저장


def excel_init():
    columns = [
        "RUN_DATE",
        "IP",
        "RUN_TIME",
        "INSP_CODE",
        "INSP_RES_CODE",
        "INSP_RES_DATA",
        "RESOLVE_PERIOD",
        "RESOLVE_METHOD",
        "RESOLVE_SCH_DATE",
        "RESOLVE_EXC_YN",
        "RESOLVE_EXC_REASON",
        "EVID_FILE_PATH",
        "EVID_FILE_ORG_NAME",
        "EVID_FILE_MOD_NAME",
    ]
    return pd.DataFrame(columns=columns)


def insert_to_data(pd_data, server_ip, now, insp_code, insp_res_code, insp_res_data):
    data = {
        "RUN_DATE": f"{now.year}-{now.month}-{now.day}",
        "IP": server_ip,
        "RUN_TIME": str(now.strftime("%Y-%m-%d %H:%M")),
        "INSP_CODE": insp_code,
        "INSP_RES_CODE": insp_res_code,
        "INSP_RES_DATA": insp_res_data,
        "RESOLVE_PERIOD": " ",
        "RESOLVE_METHOD": " ",
        "RESOLVE_SCH_DATE": " ",
        "RESOLVE_EXC_YN": "N",
        "RESOLVE_EXC_REASON": " ",
        "EVID_FILE_PATH": " ",
        "EVID_FILE_ORG_NAME": " ",
        "EVID_FILE_MOD_NAME": " ",
    }
    pd_data = pd_data._append(data, ignore_index=True)
    return pd_data


def save_to_excel(pd_data, file_path):
    # Append to existing Excel file or create a new one
    if not file_path:
        file_path = "inspection_results"
    try:
        with pd.ExcelWriter(
            file_path + ".xlsx", mode="a", engine="openpyxl", if_sheet_exists="overlay"
        ) as writer:
            pd_data.to_excel(
                writer,
                sheet_name="Sheet1",
                startcol=0,
                startrow=writer.sheets["Sheet1"].max_row,
                index=False,
                header=False,
            )
    except FileNotFoundError:
        with pd.ExcelWriter(file_path + ".xlsx", engine="openpyxl") as writer:
            pd_data.to_excel(writer, sheet_name="Sheet1", index=False, header=True)
    except Exception as e:
        print(f"Error Writting Excell {file_path} : {e}")
        logging.error(f"Error Writting Excell {file_path} : {e}")


def save_to_csv(pd_data, file_path):
    if not file_path:
        file_path = "inspection_results"
    if not os.path.exists(file_path):
        pd_data.to_csv(
            file_path + ".csv",
            sep=",",
            encoding="utf-8-sig",
            index=False,
            mode="w",
        )
    else:
        pd_data.to_csv(
            file_path + ".csv",
            sep=",",
            encoding="utf-8-sig",
            index=False,
            header=False,
            mode="a",
        )


def ndb_inspection(res, inspector):
    res = inspector.DB_01()
    res += inspector.DB_02()
    return res


# Mongodb 점검 함수
def mongodb_inspection(server):
    inspector = None
    res = None

    try:
        inspector = MongodbInspector(
            server_ip=server["ip"],
            server_port=server["port"],
            server_username=server["user"],
            server_password=server["passwd"],
        )

        res = ndb_inspection(res, inspector)

    except Exception as e:
        print(f"Error inspecting {server['dbtype']}: {e}")
        logging.error(f"Error inspecting {server['dbtype']} {server['ip']}: {str(e)}")
    finally:
        if inspector and res:
            inspector.close_connections()
        return res


def redis_inspection(server):
    inspector = None
    res = None

    try:
        inspector = RedisInspector(
            server_ip=server["ip"],
            server_port=server["port"],
            server_username=server["user"],
            server_password=server["passwd"],
        )

        res = ndb_inspection(res, inspector)

    except Exception as e:
        print(f"Error inspecting Redis: {e}")
        logging.error(
            f"Error inspecting Redis {server['ip']}: {str(e)}"
        )  # 에러 값 로그 저장
    finally:
        if inspector and res:
            inspector.close_connections()
        return res


def influxdb_inspection(server):
    inspector = None
    res = None

    try:
        inspector = InfluxdbInspector(
            server_ip=server["ip"],
            server_port=server["port"],
            server_username=server["user"],
            server_password=server["passwd"],
        )

        res = ndb_inspection(res, inspector)

    except Exception as e:
        print(f"Error inspecting Influxdb: {e}")
        logging.error(
            f"Error inspecting Influxdb {server['ip']}: {str(e)}"
        )  # 에러 값 로그 저장
    finally:
        if inspector and res:
            inspector.close_connections()
        return res


def cassandra_inspection(server):
    inspector = None
    res = None

    try:
        inspector = CassandraInspector(
            server_ip=server["ip"],
            server_port=server["port"],
            server_username=server["user"],
            server_password=server["passwd"],
            server_keyspace=server["keyspace"],
        )

        res = ndb_inspection(res, inspector)

    except Exception as e:
        print(f"Error inspecting Cassandra: {e}")
        logging.error(
            f"Error inspecting Cassandra {server['ip']}: {str(e)}"
        )  # 에러 값 로그 저장
    finally:
        if inspector and res:
            inspector.close_connections()
        return res


def inspect(server, result_queue):
    now = datetime.now()
    pd_data = excel_init()

    try:
        if server["dbtype"] == "Mongodb":
            res = mongodb_inspection(server)
        elif server["dbtype"] == "Redis":
            res = redis_inspection(server)
        elif server["dbtype"] == "Influxdb":
            res = influxdb_inspection(server)
        else:
            res = cassandra_inspection(server)

        if res is not None:
            for i in range(0, len(res), 2):
                pd_data = insert_to_data(
                    pd_data,
                    server["ip"],
                    now,
                    res[i + 1][0],
                    res[i],
                    res[i + 1][1],
                )

        result_queue.put(pd_data)

    except Exception as e:
        print(f"Error result save: {e}")
        logging.error(f"Error result save : {str(e)}")  # 에러 값 로그 저장


def main():
    parser = argparse.ArgumentParser(description="DBMS_Inspector_Tools")
    parser.add_argument("-e", "--excell", action="store_true", help="Save in excell")
    parser.add_argument("-c", "--csv", action="store_true", help="Save in csv")
    parser.add_argument(
        "-d", "--databases", action="store_true", help="Save in databases"
    )
    parser.add_argument("-f", "--file", type=str, help="Specify a file name")
    args = parser.parse_args()

    # .env 파일에서 서버 목록 불러오기
    load_dotenv()
    servers_str: Optional[str] = getenv("SERVERS")

    if servers_str is not None:
        servers = json.loads(servers_str)
        # Create a multiprocessing Queue to collect results from processes
        result_queue = multiprocessing.Manager().Queue()

        # 각 서버에 대한 작업을 병렬로 처리
        processes: List[multiprocessing.Process] = []
        for server in servers:
            process = multiprocessing.Process(
                target=inspect, args=(server, result_queue)
            )
            processes.append(process)

        # Start all processes
        for process in processes:
            process.start()

        # Wait for all processes to finish
        for process in processes:
            process.join()

        # Collect results from the queue
        results = pd.DataFrame()
        while not result_queue.empty():
            result_data = result_queue.get()
            if isinstance(result_data, pd.DataFrame):
                results = pd.concat([results, result_data], ignore_index=True)

        print("All processes completed.")
        print("Results:", results)

        save_results(results, args)
    else:
        print("SERVERS environment variable is not set.")


if __name__ == "__main__":
    main()
