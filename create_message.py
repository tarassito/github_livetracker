from dataclasses import dataclass

import reactivex
from reactivex import operators as ops, Observable


@dataclass
class Message:
    repo_name: str
    updated_time: str
    language: str
    new: bool = None

    def is_new(self, value: bool):
        self.new = value
        return self


def map_to_msg(event):
    return Message((str(event.get("full_name"))),
                   str(event.get("pushed_at")),
                   str(event.get("language")))


def process(source_of_msg: Observable[dict[str, str]]) -> Observable[Message]:
    return source_of_msg.pipe(
        ops.map(lambda x: (x.get("items"))),
        ops.flat_map(lambda x: reactivex.from_list(x)),
        ops.distinct(),
        ops.map(lambda x: map_to_msg(x))
    )
