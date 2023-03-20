import sqlite3
import uuid
from sqlite3 import Connection, Row, Cursor
from typing import Callable

import expression
import reactivex
from reactivex import scheduler, Observable, operators as ops

from create_message import Message

scheduler = scheduler.ThreadPoolScheduler(1)
connection_observable: Observable[Connection] = (reactivex.from_callable(lambda: sqlite3.connect('test.db'))
                                                 .pipe(ops.subscribe_on(scheduler)))


def execute(cmd: str) -> Callable[[Observable[Connection]], Observable[Connection]]:
    return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(cmd)),
                                      ops.map(lambda cu: cu.connection))


def query(query_string: str) -> Callable[[Observable[Connection]], Observable[Row]]:
    return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(query_string)),
                                      ops.flat_map(reactivex.from_iterable))


def commit() -> Callable[[Observable[Connection]], Observable[Connection]]:
    return lambda cn_obs: cn_obs.pipe(ops.map(lambda c: c.commit() or c))


@expression.curry(1)
def _ex(query_string: str, cn: Connection) -> Observable[Cursor]:
    return reactivex.defer(lambda s: reactivex.start(lambda: cn.execute(query_string), s))


def store_msg(c: Connection, msg: Message):
    c.execute(f"INSERT INTO github_messages (id, repository_name, language, updated_time) VALUES"
              f" ('{uuid.uuid4()}', '{msg.repo_name}', '{msg.language}', '{msg.updated_time}')")
    c.commit()


def save(source: Observable[Message]) -> Observable[None]:
    return (connection_observable.pipe(
        execute(
            '''CREATE TABLE IF NOT EXISTS github_messages
               (id varchar(48),
                repository_name varchar(128),
                language varchar(64),
                updated_time varchar(32))'''),
        ops.flat_map(
            lambda connect: source.pipe(ops.observe_on(scheduler),
                                        ops.map(lambda msg: store_msg(connect, msg)))),
        ops.ignore_elements()
    ))
