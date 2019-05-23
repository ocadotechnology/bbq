from unittest import TestCase

from src.slo.backup_creation_latency.latency_query_specification import LatencyQuerySpecification
from src.commons.config.configuration import Configuration
from mock import patch, PropertyMock


class TestLatencyQuerySpecification(TestCase):
    @patch.object(Configuration, 'projects_to_skip', new_callable=PropertyMock)
    def test_should_create_valid_query_with_skipped_projects_in_it(self, projects_to_skip):
        projects_to_skip.return_value = ['BBQ-project-id', '123']
        latency_query_spec = LatencyQuerySpecification(3)
        query = latency_query_spec.query_string()
        self.assertEqual(query,
                         "SELECT * FROM [BBQ-metadata-project-id:SLI_backup_creation_latency_views.SLI_3_days] WHERE projectId NOT IN ('BBQ-project-id', '123')")
