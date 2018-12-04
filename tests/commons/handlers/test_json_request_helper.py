from unittest import TestCase
import json

from src.commons.exceptions import JsonNotParseableException
from src.commons.handlers.json_request_helper import JsonRequestHelper


class TestJsonRequestHelper(TestCase):
    def test_cannot_parse_json(self):
        self.assertRaises(JsonNotParseableException, JsonRequestHelper.parse_request_body, '')

    def test_can_deserialize_json_to_python(self):
        # when
        result = JsonRequestHelper.parse_request_body(json.dumps({
                "tableId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))
        # then
        self.assertEqual(result, {'tableId': '64c6e50c-b511-43eb-ba75-f44f3d131f84'})
