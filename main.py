from __future__ import annotations
import os
from os.path import join, dirname
import sys
from datetime import datetime
from collections import deque
import ssl
import asyncio
from email.message import EmailMessage

import aiosmtpd
import requests
import dotenv

dotenv_path = join(dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)

class MailClient:
    _instance = None
    _initialized = False

    def __new__(cls):
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
            self._client = aiosmtpd.SMTP(hostname=self.server, port=self.port)

            # Start TLS if not on a secure port
            if self.port != 465:
                await self._client.starttls(
                    validate_certs=True, ssl_context=ssl.create_default_context()
                )

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

    async def send(self,recipient: str ,subject: str,content: str,content_type: str = "plain",) -> bool:
        if not self._connected:
            await self.connect()

        email =  EmailMessage()
        email["From"] = self.account
        email["To"] = recipient
        email["Subject"] = subject
        email.set_content(content, subtype=content_type)
        await self._client.send_message(email)
        return True


class Message:
    def __init__(self, id: str, ts: datetime, content: str):
        self.id = id
        self.ts = ts
        self.content = content

    def to_CSV_line(self) -> str:
        return f"{self.parse_date_to_csv(self.ts)},{self.id},{self.content}\n"

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
        data: tuple[str, str, str] = tuple(csv_line.split(","))
        return cls(
            ts=cls.parse_date_from_csv(data[0]),
            id=data[1],
            content=data[2],
        )

    @classmethod
    def from_full_json(cls, full_json: dict[str, str]) -> Message:
        id, ts, content = full_json["id"], full_json["timestamp"], full_json["content"]
        return cls(id=id, ts=cls.parse_date_from_json(ts), content=content)

# watched_channels = []
# for key, value in os.environ.items():
#     if key.startswith("WATCHED_CHANNEL_"):
#         watched_channels.append(value)

chann_id = os.getenv("WATCHED_CHANNEL_JOB_GENERAL")
token = os.getenv("TOKEN")
user_agent = os.getenv("USER_AGENT")
smtp_serv = os.getenv("SMTP_SERVER")
smtp_port = int(os.getenv("SMTP_PORT"))
smtp_account = os.getenv("SMTP_ACCOUNT")
smtp_pass = os.getenv("SMTP_PASSWROD")

CHANEL_PATH = f"/api/v9/channels/{chann_id}/messages?limit=50"
async def main():
    mail_client =  MailClient()
    headers = {"User-Agent": user_agent, "Authorization": token}
    url = f"https://discord.com{CHANEL_PATH}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        new_last_message = Message.from_full_json(data[0]) if data else None

        if not new_last_message:
            print("No messages found in the request.")
            sys.exit(0)

        last_line = None
        try:
            with open("last_messages.csv", "r+") as file:
                # double queue with a max len of 1 to avoid storing the whole file in the memory,
                # pass tru the file iterable and remove the old line to stay with the last line finaly
                last_line = deque(file, 10)
                if last_line:
                    last_line = last_line[0].strip()
        except FileNotFoundError:
            pass

        with open("last_messages.csv", "a+") as file:
            if last_line:
                last_file_message = Message.from_CSV_line(last_line)
                if last_file_message.id == new_last_message.id:
                    print("No new messages")
                    sys.exit(0)
            file.write(new_last_message.to_CSV_line())
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())