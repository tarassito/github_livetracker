import asyncio

from stream_data import connect
from create_message import process
from db_connector import save

import reactivex

from tornado import web, options


class Application(web.Application):
    def __init__(self):

        loop = asyncio.get_running_loop()

        connect(loop).pipe(
            process,
            lambda msg: reactivex.merge(msg, save(msg))
        ).subscribe(on_next=print, on_error=print)


async def main():
    options.parse_command_line()
    app = Application()
    app.listen(8080)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
