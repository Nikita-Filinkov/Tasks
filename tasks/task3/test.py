# from clickhouse_driver import Client
#
# def test_connection():
#     try:
#         client = Client(
#             host='localhost',
#             port=8123,
#             user='default',
#             password='',
#             database='default',
#             secure=False
#         )
#         result = client.execute("SELECT 1")
#         print("Успешное подключение! Результат:", result)
#         client.disconnect()
#     except Exception as e:
#         print("Ошибка подключения:", e)
#
# if __name__ == "__main__":
#     test_connection()
