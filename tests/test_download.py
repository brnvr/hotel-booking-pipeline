"""Test reuse, integrity checks, and safe publication without network access."""

import contextlib
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import download as downloader


class DownloadTests(unittest.TestCase):
    def client(self, source):
        client = Mock()
        client.dataset_download.return_value = str(source)
        return client

    def test_existing_file_skips_client_and_preserves_local_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / downloader.FILE_NAME
            destination.write_bytes(b"existing data")
            client = self.client(root / "unused")
            with patch.dict("sys.modules", {"kagglehub": client}):
                self.assertEqual(downloader.download(root), destination)
            client.dataset_download.assert_not_called()
            self.assertEqual(destination.read_bytes(), b"existing data")

    def test_missing_or_empty_file_downloads_and_normalizes_line_endings(self):
        for empty in (False, True):
            with self.subTest(empty=empty), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                output = root / "input"
                if empty:
                    output.mkdir()
                    (output / downloader.FILE_NAME).touch()
                source = root / "cached.csv"
                source.write_bytes(b"hotel,adr\r\nCity Hotel,100\r\n")
                content = source.read_bytes().replace(b"\r\n", b"\n")
                client = self.client(source)
                with (
                    patch.dict("sys.modules", {"kagglehub": client}),
                    patch.object(downloader, "EXPECTED_SHA256",
                                 hashlib.sha256(content).hexdigest()),
                ):
                    result = downloader.download(output)
                self.assertEqual(result.read_bytes(), content)
                client.dataset_download.assert_called_once_with(
                    downloader.DATASET, path=downloader.FILE_NAME,
                    force_download=False,
                )
                self.assertEqual(list(output.iterdir()), [result])

    def test_force_replaces_existing_file_only_after_verification(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / downloader.FILE_NAME
            destination.write_bytes(b"old data")
            source = root / "cached.csv"
            content = b"new verified data"
            source.write_bytes(content)
            client = self.client(source)
            with (
                patch.dict("sys.modules", {"kagglehub": client}),
                patch.object(downloader, "EXPECTED_SHA256",
                             hashlib.sha256(content).hexdigest()),
            ):
                downloader.download(root, force=True)
            self.assertEqual(destination.read_bytes(), content)
            self.assertTrue(client.dataset_download.call_args.kwargs["force_download"])

    def test_invalid_download_does_not_publish_or_replace_input(self):
        for existing in (False, True):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                destination = root / downloader.FILE_NAME
                if existing:
                    destination.write_bytes(b"old data")
                source = root / "cached.csv"
                source.write_bytes(b"invalid content")
                with patch.dict("sys.modules", {"kagglehub": self.client(source)}):
                    with self.assertRaisesRegex(ValueError, "checksum"):
                        downloader.download(root, force=True)
                if existing:
                    self.assertEqual(destination.read_bytes(), b"old data")
                else:
                    self.assertFalse(destination.exists())

    def test_client_failure_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / downloader.FILE_NAME
            destination.write_bytes(b"old data")
            client = self.client(root / "unused")
            client.dataset_download.side_effect = ConnectionError("offline")
            with patch.dict("sys.modules", {"kagglehub": client}):
                with self.assertRaises(ConnectionError):
                    downloader.download(root, force=True)
            self.assertEqual(destination.read_bytes(), b"old data")

    def test_cli_reports_failure_without_echoing_sensitive_error_text(self):
        stderr = io.StringIO()
        with (
            patch("sys.argv", ["download", "--input-dir", "input"]),
            patch.object(downloader, "download", side_effect=RuntimeError("secret")),
            contextlib.redirect_stderr(stderr),
        ):
            with self.assertRaises(SystemExit) as error:
                downloader.main()
        self.assertEqual(error.exception.code, 1)
        self.assertIn("Download failed", stderr.getvalue())
        self.assertNotIn("secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
