import httpx
import asyncio
import time
import random
import ssl
import os
from db import Database

if os.name == 'nt':  # 'nt' означает Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

RPS = 5
TIMEOUT = 10
MAX_REQUESTS = 100
BASE_URL = 'https://api.uzum.uz/api/v2/product/{}'
HEADERS = {
    'authority': 'api.uzum.uz',
    'accept': 'application/json',
    'accept-language': 'ru-RU',
    'content-type': 'application/json',
    'origin': 'https://uzum.uz',
    'referer': 'https://uzum.uz/',
    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'macOS',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-iid': '7861ccfa-be32-401f-adcf-a3adb2a86225'
}


async def send_requests(rps):
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        start_time = time.perf_counter()
        status_counter = {
            'Requests': 0,
            200: 0,
            429: 0
        }
        req_count = rps
        while status_counter[429] == 0 and status_counter['Requests'] < MAX_REQUESTS:

            tasks = [fetch_data(client, random.randint(1, 1000000)) for _ in range(req_count)]
            responses = await asyncio.gather(*tasks)

            for response in responses:
                status = response.status_code if isinstance(response, httpx.Response) else 'error'
                status_counter[status] = status_counter.get(status, 0) + 1
                status_counter['Requests'] += 1
                if status == 200:
                    db.extract_and_insert_data(response)

            response_time = time.perf_counter() - start_time
            if status_counter[200] / response_time < rps:
                req_count = min(5, req_count + 1)
                # print(req_count)
            else:
                req_count = max(1, req_count - 1)
                if req_count == 1:
                    await asyncio.sleep(1)
                # print(req_count)
        status_counter['Total time'] = response_time
        status_counter['RPS'] = status_counter[200] / response_time

        return status_counter


async def fetch_data(client, product_id):
    try:
        response = await client.get(BASE_URL.format(product_id), headers=HEADERS)
        return response
    except (ssl.SSLError, httpx.RequestError, httpx.HTTPStatusError, ssl.SSLCertVerificationError):
        return "error"


async def main():
    print(await send_requests(RPS))


if __name__ == '__main__':
    db = Database()
    asyncio.run(main())
