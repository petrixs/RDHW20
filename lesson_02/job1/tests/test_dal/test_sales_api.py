import os
from unittest import TestCase, mock
from lesson_02.job1.dal.sales_api import get_sales


class GetSalesTestCase(TestCase):

    @mock.patch("lesson_02.job1.dal.sales_api.requests.get")
    def test_get_sales_success_multiple_pages(self, mock_get):
        def side_effect(*args, **kwargs):
            page = kwargs["params"]["page"]
            if page == 1:
                return mock.Mock(status_code=200, json=lambda: [{"sale_id": 1, "amount": 100}])
            elif page == 2:
                return mock.Mock(status_code=200, json=lambda: [{"sale_id": 2, "amount": 200}])
            else:
                return mock.Mock(status_code=200, json=lambda: [])

        mock_get.side_effect = side_effect

        sales_data = get_sales("2023-01-01")
        self.assertEqual(len(sales_data), 2)
        self.assertEqual(sales_data[0]["sale_id"], 1)
        self.assertEqual(sales_data[1]["sale_id"], 2)
        self.assertEqual(mock_get.call_count, 3)

    @mock.patch("lesson_02.job1.dal.sales_api.requests.get")
    def test_get_sales_http_error(self, mock_get):
        mock_get.return_value.status_code = 500

        sales_data = get_sales("2023-01-01")
        self.assertEqual(len(sales_data), 0)
        mock_get.assert_called_once()
