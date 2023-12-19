"""
Tests for main.py
# TODO: write tests
"""
from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch('lesson_02.job2.main.save_sales_to_local_disk')
    def test_return_400_date_param_missed(
            self,
            get_sales_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': 'some/non/existent/path/file.json',
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        pass

    @mock.patch('lesson_02.job2.main.save_sales_to_local_disk')
    @mock.patch('os.path.exists', return_value=True)
    def test_save_sales_to_local_disk(
            self,
            mock_exists,
            save_sales_to_local_disk_mock: mock.MagicMock
    ):

        fake_stg_dir = 'some/non/existent/path/file.json'
        fake_raw_dir = 'some/non/existent/path/file.json'
        self.client.post(
            '/',
            json={
                'stg_dir': fake_stg_dir,
                'raw_dir': fake_raw_dir,
            },
        )

        save_sales_to_local_disk_mock.assert_called_with(
            stg_dir=fake_stg_dir,
            raw_dir=fake_raw_dir,
        )

    @mock.patch('lesson_02.job2.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        pass
