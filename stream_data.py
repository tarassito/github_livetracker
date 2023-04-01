from typing import Union

import aiohttp
import reactivex
import asyncio

from reactivex import Observable
from reactivex.abc import ObserverBase
from reactivex.disposable import Disposable


async def fetch_data(observer, keyword):
    async with aiohttp.ClientSession() as session:
        headers = {
            'Accept': 'application/vnd.github.text-match+json',
            'Authorization': 'Bearer <TOKEN_PLACEHOLDER>',
            'X-GitHub-Api-Version': '2022-11-28'
        }
        while True:
            async with session.get(f'https://api.github.com/search/repositories?sort=updated&q={keyword}&per_page=3',
                                   headers=headers) as resp:
                res = await resp.json()
                observer.on_next(res)
                await asyncio.sleep(10)


def connect(subscription_loop: asyncio.AbstractEventLoop, keyword: str) -> Observable[dict[str, Union[str, float]]]:
    def on_subscribe(observer: ObserverBase, scheduler):
        task = asyncio.run_coroutine_threadsafe(fetch_data(observer, keyword=keyword), loop=subscription_loop)

        return Disposable(lambda: task.cancel())
    return reactivex.create(on_subscribe)

