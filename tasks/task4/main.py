import asyncio

from tasks.task4.database_init import DatabaseInitializer


async def main():
    initializer = DatabaseInitializer()
    await initializer.initialize()
    data = await initializer.get_data()
    return data


if __name__ == '__main__':
    result = asyncio.run(main())
    print(result)