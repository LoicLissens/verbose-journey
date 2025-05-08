from __future__ import annotations
import os
from os.path import join, dirname
import sys
from datetime import datetime
from collections import deque
from typing import Union
import asyncio
from email.message import EmailMessage
import base64
import logging
from zoneinfo import ZoneInfo

from aiosmtplib import SMTP
import requests
import dotenv

class MailClient:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MailClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, server: str, port: int, account: str, password: str):
        if not MailClient._initialized:
            self.server = server
            self.port = port
            self.account = account
            self.password = password
            self._client = None
            self._connected = False
            MailClient._initialized = True

    async def connect(self):
        if not self._connected:
            self._client = SMTP(hostname=self.server, port=self.port, use_tls=True)
            await self._client.connect()
            await self._client.login(self.account, self.password)
            self._connected = True

    async def disconnect(self) -> None:
        if self._connected and self._client:
            try:
                await self._client.quit()
            except Exception:
                if hasattr(self._client, "close"):
                    self._client.close()
            finally:
                self._connected = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self.disconnect()
        return self

    async def send(
        self,
        recipient: str,
        subject: str,
        content: str,
        content_type: str = "plain",
    ) -> bool:
        if not self._connected:
            await self.connect()

        email = EmailMessage()
        email["From"] = self.account
        email["To"] = recipient
        email["Subject"] = subject
        email.set_content(content, subtype=content_type)
        await self._client.send_message(email)
        return True

class Message:
    def __init__(self, id: str, ts: datetime, content: str, channel_id: str,channel_name:str):
        """
        Initialize a new message instance.

        Args:
            id (str): The unique identifier of the message.
            ts (datetime): The timestamp when the message was created.
            content (str): The content of the message.
            channel_id (str): The identifier of the channel where the message belongs.

        Properties:
            id (str): The unique identifier of the message.
            ts (datetime): The timestamp when the message was created.
            content (str): The content of the message.
            channel_id (str): The identifier of the channel where the message belongs.
        """
        self.id = id
        self.ts = ts
        self.content = content.strip().replace("\n","")
        self.channel_id = channel_id
        self.channel_name =  channel_name

    def to_CSV_line(self) -> str:
        return f"{self.parse_date_to_csv(self.ts)};{self.id};{self.content};{self.channel_id};{self.channel_name}\n"

    def to_mail_format(self) -> str:
        posted_time = self.ts.astimezone(ZoneInfo("Europe/Brussels")).strftime(
            "%d/%m/%Y, %H:%M:%S"
        )
        return f"In {self.channel_name} at {posted_time} : {self.content}\n\n"

    @staticmethod
    def parse_date_from_csv(str_date) -> datetime:
        return datetime.strptime(str_date, "%d-%m-%YT%H:%M:%S.%f%z")

    @staticmethod
    def parse_date_from_json(str_date) -> datetime:
        return datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%S.%f%z")

    @staticmethod
    def parse_date_to_csv(ts: datetime) -> str:
        return ts.strftime("%d-%m-%YT%H:%M:%S.%f%z")

    @classmethod
    def from_CSV_line(cls, csv_line: str) -> Message:
        data: tuple[str, str, str, str] = tuple(csv_line.split(";"))
        return cls(
            ts=cls.parse_date_from_csv(data[0]),
            id=data[1],
            content=data[2],
            channel_id=data[3],
            channel_name=data[4]
        )

    @classmethod
    def from_full_json(cls, full_json: dict[str, str],channel_name) -> Message:
        id, ts, content, chann_id = (
            full_json["id"],
            full_json["timestamp"],
            full_json["content"],
            full_json["channel_id"],
        )
        return cls(
            id=id, ts=cls.parse_date_from_json(ts), content=content, channel_id=chann_id,channel_name=channel_name
        )

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


dotenv_path = join(dirname(__file__), ".env")
dotenv.load_dotenv(dotenv_path)


base_url = os.getenv("BASE_URL")
token = os.getenv("TOKEN")
user_agent = os.getenv("USER_AGENT")

smtp_serv = os.getenv("SMTP_SERVER")
smtp_port = int(os.getenv("SMTP_PORT"))
smtp_account = os.getenv("SMTP_ACCOUNT")
smtp_pass = base64.b64decode(os.getenv("SMTP_PASSWROD")).decode("ascii")
smtp_dest = os.getenv("SMTP_DEST")

async def send_mail(client: MailClient, messages: list[Message]):
    subject = f"{len(messages)} new jobs found"
    content = ""
    for msg in messages:
        content += msg.to_mail_format()
    async with client as mail:
        await mail.send(smtp_dest, subject=subject, content=content)
    logger.info(f"{subject} sending by email.")

async def get_last_msg(chann: dict[str,str]):
    chann_path = f"/api/v9/channels/{chann["id"]}/messages?limit=50"
    url = f"{base_url}{chann_path}"
    response = requests.get(
        url, headers={"User-Agent": user_agent, "Authorization": token}
    )
    if response.status_code == 200:
        data = response.json()
        new_fetched_message = Message.from_full_json(data[0],chann["name"]) if data else None
        if not new_fetched_message:
            logger.info("No messages found in the request.")
    else:
        logger.error(
            f"Request failed with status code: {response.status_code}. Msg : {response.text}"
        )
        sys.exit(1)
    return new_fetched_message

def handle_file(new_fetched_message: Message,  chann:dict[str,str]) -> Union[Message, None]:
    try:
        filename = f"last_messages_{chann["name"]}.csv"
        last_line = None
        with open(filename, "r+") as file:
            # double queue with a max len of 1 to avoid storing the whole file in the memory,
            # pass tru the file iterable and remove the old line to stay with the last line finaly
            last_line = deque(file, 1)
            if last_line:
                last_line = last_line[0].strip()
    except FileNotFoundError:
        pass
    with open(filename, "a+") as file:
        if last_line:
            last_file_message = Message.from_CSV_line(last_line)
            if last_file_message.id == new_fetched_message.id:
                logger.info("No new messages")
                return
        file.write(new_fetched_message.to_CSV_line())
        return new_fetched_message

async def main():
    watched_channels = []
    for key, value in os.environ.items():
        if key.startswith("WATCHED_CHANNEL_"):
            watched_channels.append(value)
    messages_to_send: list[Message] = []

    for channel in watched_channels:
        # Could do that with asyncio gather but maybe I'll get a ban for fireing multiple request at the same times
        chann_dict = {"id":channel.split("-")[0],"name":channel.split("-")[1]}
        new_fetched_message = await get_last_msg(chann_dict)
        new_written_message = handle_file(new_fetched_message, chann_dict)
        if new_written_message:
            messages_to_send.append(new_written_message)

    if messages_to_send:
        mail_client = MailClient(
            server=smtp_serv, port=smtp_port, account=smtp_account, password=smtp_pass
        )
        await send_mail(mail_client, messages_to_send)

if __name__ == "__main__":
    asyncio.run(main())
