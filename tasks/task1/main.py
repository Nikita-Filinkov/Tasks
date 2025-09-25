import uvicorn
from fastapi import FastAPI

from tasks.task1.router import register_routes


def create_app() -> FastAPI:
    """Создание приложения"""
    app = FastAPI(
        title="e-Comet",
        description="API для получения информации о версии PostgreSQL",
        version="1.0.0"
    )
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
