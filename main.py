from asyncio import run
from anime_downloader import AnimeDownloader

if __name__ == '__main__':
    downloader = AnimeDownloader('config.json')
    run(downloader.main())
