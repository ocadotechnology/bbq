from unittest import TestCase

from src.slo.backup_quality.quality_query_specification import QualityQuerySpecification
from src.commons.config.configuration import Configuration
from mock import patch, PropertyMock


class TestQualityQuerySpecification(TestCase):
    @patch.object(Configuration, 'projects_to_skip', return_value=['BBQ-project-id', 'ta-testing-project', '123'],
                  new_callable=PropertyMock)
    def test_should_create_valid_query_with_skipped_projects_in_it(self, projects_to_skip):
        projects_to_skip.return_value = ['BBQ-project-id', 'ta-testing-project', '123']
        quality_query_spec = QualityQuerySpecification()
        query = quality_query_spec.query_string()
        self.assertEqual(query,
                         "SELECT * FROM [BBQ-project-id:SLI_backup_quality_views.SLI_quality] WHERE projectId NOT IN ('BBQ-project-id', 'ta-testing-project', '123')")
