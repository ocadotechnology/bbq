resource "google_bigquery_dataset" "SLO_views_legacy_dataset" {
  dataset_id = "SLO_views_legacy"
  project = "${var.bbq_project}"
  location = "EU"
}

resource "google_bigquery_table" "census_data_7_days_ago_view" {
  project = "${var.bbq_project}"
  dataset_id = "SLO_views_legacy"
  table_id = "census_data_7_days_ago"

  view {
    query = <<EOF
          #legacySQL
            -- Shows all tables and partitions seen by census 7 days ago
          SELECT * FROM (
            SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
            FROM (
              SELECT
                projectId, datasetId, tableId, creationTime, lastModifiedTime, 'null' AS partitionId,
                ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
              FROM
                [${var.census_project}.bigquery.table_metadata_v1_0]
              WHERE
                _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -10, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -7, "DAY"))
                AND timePartitioning.type IS NULL AND type='TABLE'
            )
            WHERE rownum = 1
          ), (
            SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
            FROM (
              SELECT
                projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime,
                  ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId, partitionId ORDER BY snapshotTime DESC) AS rownum
                FROM
                  [${var.census_project}.bigquery.partition_metadata_v1_0]
                WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -10, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -7, "DAY"))
            )
            WHERE rownum = 1
          )
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLO_views_legacy_dataset"]
}