import asyncio
import logging
from typing import Any


from stream_data import connect
from create_message import process, Message
from db_connector import save

import reactivex

from tornado import web, options, websocket

options.define("port", default=8080, help="run on the given port", type=int)


class Application(web.Application):
    def __init__(self):
        handlers = [(r"/stream", StreamSocketHandler)]
        super().__init__(handlers)

        loop = asyncio.get_running_loop()

        connect(loop, keyword="python").pipe(
            process,
            lambda msg: reactivex.merge(msg, save(msg))
        ).subscribe(on_next=StreamSocketHandler.send_updates, on_error=print)


class StreamSocketHandler(websocket.WebSocketHandler):
    waiters: set[Any] = set()

    def open(self):
        logging.info('Hello friend')
        StreamSocketHandler.waiters.add(self)

    def on_close(self):
        StreamSocketHandler.waiters.remove(self)

    @classmethod
    def send_updates(cls, msg: Message):
        logging.info("sending message to %d waiters", len(cls.waiters))
        for waiter in cls.waiters:
            try:
                waiter.write_message({
                    "repository_name": msg.repo_name,
                    "updated": msg.updated_time,
                    "language": msg.language
                }, binary=False)
            except:
                logging.error("Error sending message", exc_info=True)


async def main():
    options.parse_command_line()
    app = Application()
    app.listen(8080)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
