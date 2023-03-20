from dataclasses import dataclass

import reactivex
from reactivex import operators as ops, Observable


@dataclass
class Message:
    repo_name: str
    updated_time: str
    language: str


def map_to_msg(event):
    return Message((str(event.get("full_name"))),
                   str(event.get("pushed_at")),
                   str(event.get("language")))


def process(source_of_msg: Observable[dict[str, str]]) -> Observable[Message]:
    return source_of_msg.pipe(
        ops.map(lambda x: (x["items"])),
        ops.flat_map(lambda x: reactivex.from_list(x)),
        ops.distinct(),
        ops.map(lambda x: map_to_msg(x))
    )
