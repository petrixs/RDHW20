import json
import os
from unittest import TestCase, mock
from lesson_02.job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    @mock.patch("os.makedirs")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_save_to_disk_creates_directory(self, mock_open, mock_makedirs):
        json_content = [{"key": "value"}]
        path = "some/non/existent/path/file.json"

        save_to_disk(json_content, path)

        mock_makedirs.assert_called_once_with(os.path.dirname(path))
        mock_open.assert_called_once_with(path, 'w', encoding='utf-8')

    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_save_to_disk_existing_directory(self, mock_open, mock_path_exists):
        json_content = [{"key": "value"}]
        path = "existing/path/file.json"

        save_to_disk(json_content, path)

        mock_path_exists.assert_called_once_with(os.path.dirname(path))
        mock_open.assert_called_once_with(path, 'w', encoding='utf-8')

    @mock.patch("json.dump")
    @mock.patch("os.makedirs")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_save_to_disk_writes_content(self, mock_open, mock_makedirs, mock_json_dump):
        json_content = [{"key": "value"}]
        path = "some/path/file.json"

        save_to_disk(json_content, path)

        mock_json_dump.assert_called_once_with(json_content, mock_open())

    def test_save_to_disk_invalid_content(self):

        with self.assertRaises(TypeError):
            save_to_disk(None, "some/path/file.json")

        with mock.patch("builtins.open", new_callable=mock.mock_open):
            save_to_disk([], "some/path/file.json")