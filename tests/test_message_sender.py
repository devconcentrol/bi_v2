import unittest
from unittest.mock import MagicMock, patch
from urllib import parse

import pandas as pd

from utils.message_sender import EmailSender, WhatsAppSender
from utils.result_sender import ProcessExecutionNotifier


class WhatsAppSenderTests(unittest.TestCase):
    @patch("utils.message_sender.request.urlopen")
    def test_send_calls_api_with_encoded_message(self, urlopen):
        response = MagicMock()
        response.getcode.return_value = 200
        urlopen.return_value.__enter__.return_value = response

        WhatsAppSender("https://example.test/send", "secret").send("Hello world")

        requested_url = urlopen.call_args.args[0]
        query = parse.parse_qs(parse.urlsplit(requested_url).query)
        self.assertEqual(query, {"text": ["Hello world"], "apikey": ["secret"]})
        urlopen.assert_called_once_with(requested_url, timeout=30)

    def test_send_requires_configuration(self):
        with self.assertRaises(ValueError):
            WhatsAppSender(None, "secret").send("message")


class EmailSenderTests(unittest.TestCase):
    @patch("utils.message_sender.smtplib.SMTP")
    def test_send_builds_html_email(self, smtp_cls):
        smtp = smtp_cls.return_value.__enter__.return_value
        sender = EmailSender(
            "smtp.example.test",
            "sender@example.test",
            ["one@example.test", "two@example.test"],
            "ETL report",
            username="user",
            password="password",
        )

        sender.send("<h1>Everything is fine</h1>")

        smtp_cls.assert_called_once_with("smtp.example.test", 587, timeout=30)
        smtp.starttls.assert_called_once_with()
        smtp.login.assert_called_once_with("user", "password")
        email = smtp.send_message.call_args.args[0]
        self.assertEqual(email.get_content_type(), "text/html")
        self.assertEqual(email["To"], "one@example.test, two@example.test")
        self.assertIn("<h1>Everything is fine</h1>", email.get_content())


class ResultSenderTests(unittest.TestCase):
    @patch("utils.result_sender.Config.get_instance")
    @patch("utils.result_sender.pd.read_sql")
    def test_send_result_uses_injected_sender(self, read_sql, get_config):
        get_config.return_value = MagicMock()
        read_sql.return_value = pd.DataFrame({"ETL": ["Job failed"]})
        message_sender = MagicMock()

        ProcessExecutionNotifier(MagicMock(), message_sender).send_result()

        message_sender.send.assert_called_once_with("Execution results:\nJob failed")


if __name__ == "__main__":
    unittest.main()
