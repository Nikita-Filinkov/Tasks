from fastapi import HTTPException, status


class Task1Exception(HTTPException):
    status_code = 500
    detail = ""

    def __init__(self):
        super().__init__(status_code=self.status_code, detail=self.detail)


class BaseConnException(Task1Exception):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    detail = "Не удалось подключиться к базе данных"


class UnexpectedException(Task1Exception):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    detail = "Внутренняя ошибка сервера"


class BaseRequestException(Task1Exception):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    detail = "Ошибка при выполнении запроса к базе данных"


