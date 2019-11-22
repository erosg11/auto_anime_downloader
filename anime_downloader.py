from telegram import Bot
import requests_async
import logging
from pget.down import Downloader
from json import load
from glob import iglob
from os.path import join, split
from syncasync import sync_to_async
from aiostream.stream import starmap, iterate
from asyncio import sleep, gather
from sys import stdout


class AnimeDownloader(object):
    bot: Bot
    tasks: dict
    config_file: str
    chat_id: int
    root_path: str
    message_retry: int
    chunck_count: int
    notifications: float
    sleep_time: float
    task_limit: int

    def __init__(self, config_file: str):
        with open(config_file) as fp:
            config = load(fp)
        self.bot = Bot(config["bot_key"])
        self.config_file = config_file
        self.chat_id = config['chat_id']
        self.root_path = config['root_path']
        self.tasks = {}
        self.message_retry = config['message_retry']
        self.chunck_count = config['chunck_count']
        self.notifications = config['notifications']
        self.sleep_time = config['sleep']
        self.task_limit = config['task_limit']
        self.log = logging.getLogger()
        handle = logging.StreamHandler(stdout)
        handle.setLevel('DEBUG')
        handle.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] (%(module)s) -> (%(funcName)s) -> '
                                              '(%(lineno)d) : %(message)s'))
        self.log.addHandler(handle)
        self.log.setLevel(config['log_level'])

    async def main(self):
        await gather(self.runner(), self.monitor_runner())

    async def runner(self):
        while True:
            tasks = []
            with open(self.config_file) as fp:
                config = load(fp)
            urls = config['urls']
            animes_path = ((anime, join(self.root_path, anime)) for anime in config['urls'].keys())
            self.log.debug("Paths found %s", animes_path)
            for anime, anime_path in animes_path:
                downloadeds = set((int(split(x)[1][:-4]) for x in iglob(join(anime_path, '*.mp4'))))
                last = max(downloadeds)
                not_found = sorted(set(range(1, last)) - downloadeds)
                apped_not_found = not_found.append
                next_ok = True
                while next_ok:
                    last += 1
                    if await self.check_ok(urls[anime] % last):
                        apped_not_found(last)
                    else:
                        self.log.info("Episode %d not found for %s", last, anime)
                        next_ok = False
                self.log.debug("Not founds %s", not_found)
                tasks += [(anime, num, urls[anime], anime_path) for num in not_found]
                if not not_found:
                    self.log.info("No new episode for %s", anime)
                else:
                    await self.send_message_async(
                        f"New episodes for {anime}:\n    {', '.join((str(x) for x in not_found))}")
            async with starmap(iterate(tasks), self.task_job
                    , task_limit=self.task_limit).stream() as stream:
                async for _ in stream:
                    pass
            await sleep(self.sleep_time)

    @staticmethod
    async def check_ok(url):
        try:
            resp = await requests_async.head(url)
            assert resp.ok
        except:
            return False
        return True

    async def task_job(self, anime, episde_num, url, base_path):
        final_url = url % episde_num
        self.log.debug("Task for %s episode %d", anime, episde_num)
        if not await self.check_ok(final_url):
            self.log.error(f"Failed to fetch data of episode {episde_num} of {anime}")
            return
        downloader = Downloader(final_url, join(base_path, f'{episde_num:03}.mp4'), self.chunck_count)
        self.tasks[(anime, episde_num)] = downloader
        downloader.start()
        self.log.debug("Downloading %s episode %d", anime, episde_num)
        await sync_to_async(downloader.wait_for_finish)()
        await self.send_message_async(f"The episode {episde_num} of {anime} was downloaded")
        del self.tasks[(anime, episde_num)]

    async def monitor_runner(self):
        while True:
            if self.tasks:
                messages = [f"The episode {episode_num} of {anime} is downloading"
                            f"""\n    Downloaded: {donwloader.total_downloaded /
                                                   donwloader.total_length * 100 if donwloader.total_length
                            else 100.:.2f}%"""
                            f"\n    Speed: {donwloader.readable_speed}"
                            for (anime, episode_num), donwloader in sorted(self.tasks.items())
                            ]
                await self.send_message_async('\n\n'.join(messages), disable_notification=True)
            await sleep(self.notifications)

    async def send_message_async(self, *args, **kwargs):
        await sync_to_async(lambda: self.send_message(*args, **kwargs))()

    def send_message(self, *args, **kwargs):
        self.log.debug("Sending message %s", args[0])
        for _ in range(self.message_retry):
            try:
                self.bot.send_message(self.chat_id, *args, **kwargs)
            except TimeoutError:
                pass
            else:
                break

    def callback_gen(self, episode: str, key: tuple):
        def donwloader_callback(donwloader: Downloader):
            if donwloader.total_merged == donwloader.total_length:
                self.send_message(f"The episode {episode} was downloaded")
                del self.tasks[key]
            else:
                self.send_message(f"The episode {episode} is downloading"
                                  f"""\n    Downloaded: {donwloader.total_downloaded /
                                                         donwloader.total_length * 100:.2f}%"""
                                  f"    Speed: {donwloader.readable_speed}")

        return donwloader_callback
