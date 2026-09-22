import smtplib
from abc import ABC, abstractmethod
from email.message import EmailMessage
from urllib import error, parse, request

from utils.logger import Logger


class MessageSender(ABC):
    """Contract for message delivery channels."""

    @abstractmethod
    def send(self, message: str) -> None:
        """Send a message through the configured channel."""


class WhatsAppSender(MessageSender):
    def __init__(self, api_url: str | None, api_key: str | None):
        self._api_url = api_url
        self._api_key = api_key

    def send(self, message: str) -> None:
        if not self._api_key:
            raise ValueError("API_KEY not set in environment.")
        if not self._api_url:
            raise ValueError("API_URL not set in environment.")

        query_params = parse.urlencode(
            {
                "text": message,
                "apikey": self._api_key,
            }
        )
        separator = "&" if "?" in self._api_url else "?"
        api_url = f"{self._api_url}{separator}{query_params}"

        try:
            with request.urlopen(api_url, timeout=30) as response:
                status_code = response.getcode()
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="ignore")
            Logger().error(
                "API request failed with status %s: %s",
                exc.code,
                response_body,
            )
            raise
        except error.URLError as exc:
            Logger().error("Could not connect to API: %s", exc.reason)
            raise

        Logger().info("Message sent successfully. Status code: %s", status_code)


class EmailSender(MessageSender):
    """Send HTML messages through an SMTP server."""

    def __init__(
        self,
        smtp_host: str,
        sender: str,
        recipients: str | list[str] | tuple[str, ...],
        subject: str,
        *,
        smtp_port: int = 587,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
    ):
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._sender = sender
        self._recipients = (
            [recipients] if isinstance(recipients, str) else list(recipients)
        )
        self._subject = subject
        self._username = username
        self._password = password
        self._use_tls = use_tls

    def send(self, message: str) -> None:
        email = EmailMessage()
        email["From"] = self._sender
        email["To"] = ", ".join(self._recipients)
        email["Subject"] = self._subject
        email.set_content(message, subtype="html")

        with smtplib.SMTP(
            self._smtp_host,
            self._smtp_port,
            timeout=30,
        ) as smtp:
            if self._use_tls:
                smtp.starttls()
            if self._username:
                smtp.login(self._username, self._password or "")
            smtp.send_message(email)

        Logger().info("Email sent successfully to %s", email["To"])
