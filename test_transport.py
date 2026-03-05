import unittest
from unittest.mock import patch, MagicMock
from spidermanager_sdk.transport import HttpTransport, _INGEST_PATH

class TestTransport(unittest.TestCase):
    @patch("httpx.Client.post")
    def test_send_batch_url(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        transport = HttpTransport(api_url="http://test_backend:8000", task_id="test_task_123")
        
        # 验证发送批量数据时使用的 URL 是否正确
        success = transport.send_batch("test_table", [{"col1": "val1"}])
        
        self.assertTrue(success)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        
        self.assertEqual(args[0], "/api/tasks/data/ingest")
        self.assertEqual(kwargs["json"], {"table_name": "test_table", "data": [{"col1": "val1"}]})
        self.assertEqual(kwargs["params"], {"task_id": "test_task_123"})
        print("Test passed: URL is correctly formed as /api/tasks/data/ingest")

if __name__ == "__main__":
    unittest.main()
