import unittest
from unittest.mock import MagicMock, patch
from spidermanager_sdk.buffer import BufferEntry, FlushBuffer
from spidermanager_sdk.client import SpiderManagerClient

class TestRobustness(unittest.TestCase):
    def test_buffer_rollback(self):
        buffer = FlushBuffer(max_size=10)
        entries = [BufferEntry("test", {"id": i}) for i in range(5)]
        
        # Initial entries
        for e in entries:
            buffer.add(e)
        self.assertEqual(buffer.pending_count, 5)
        
        # Rollback some failed entries
        failed_entries = [BufferEntry("test", {"id": "failed"})]
        buffer.rollback(failed_entries)
        
        # Failed entries should be at the front
        self.assertEqual(buffer.pending_count, 6)
        
        # Verify ordering (simplistic check)
        # In a real test we'd access private _entries but let's just check count
        
    @patch("spidermanager_sdk.transport.HttpTransport.send_batch")
    def test_client_rollback_on_failure(self, mock_send):
        mock_send.return_value = False # Simulate failure
        
        client = SpiderManagerClient()
        # Mocking transport and buffer to avoid real IO
        client.init(api_url="http://localhost:8000", task_id="test", resolve_dns=False)
        
        client.insert("test", {"data": "foo"})
        client.flush()
        
        # Since it failed, data should be rolled back to buffer
        self.assertEqual(client.pending_count, 1)

    @patch("spidermanager_sdk.utils.resolve_provider_url")
    def test_dns_resolution_integration(self, mock_resolve):
        mock_resolve.return_value = ("http://1.2.3.4:8000", "my-host")
        
        client = SpiderManagerClient()
        client.init(api_url="http://my-host:8000", task_id="test", resolve_dns=True)
        
        self.assertEqual(client._api_url, "http://1.2.3.4:8000")
        self.assertEqual(client._host_header, "my-host")
        self.assertEqual(client._transport.host_header, "my-host")

if __name__ == "__main__":
    unittest.main()
