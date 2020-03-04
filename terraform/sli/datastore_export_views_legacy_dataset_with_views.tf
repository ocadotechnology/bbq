resource "google_bigquery_dataset" "datastore_export_views_legacy_view" {
  dataset_id = var.datastore_export_views_dataset
  project = local.datastore_export_project
  location = var.SLI_views_location

  labels = {
    "bbq_metadata" = ""
  }
}

resource "google_bigquery_table" "last_table_view" {
  project = local.datastore_export_project
  dataset_id = var.datastore_export_views_dataset
  table_id = "last_table"

  view {
    query = <<EOF
        SELECT
          project_id,
          dataset_id,
          table_id,
          IFNULL(partition_id,'None') as partition_id,
          last_checked,
          __key__.id AS id
        FROM
          TABLE_QUERY( [${google_bigquery_dataset.datastore_export_dataset.id}],
            'table_id=(SELECT MAX(table_id)
                       FROM [${google_bigquery_dataset.datastore_export_dataset.id}.__TABLES__]
                       WHERE LEFT(table_id, 6) = "Table_")' )
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "last_backup_view" {
  project = local.datastore_export_project
  dataset_id = var.datastore_export_views_dataset
  table_id = "last_backup"

  view {
    query = <<EOF
          SELECT
            created,
            deleted,
            last_modified,
            numBytes,
            table_id,
            dataset_id,
            key,
            INTEGER(parent_id) AS parent_id
          FROM (
            SELECT
              created,
              deleted,
              last_modified,
              numBytes,
              table_id,
              dataset_id,
              NTH(2, SPLIT(__key__.path, ',')) AS parent_id,
              TO_BASE64(BYTES(__key__.path)) AS key
            FROM
              TABLE_QUERY( [${google_bigquery_dataset.datastore_export_dataset.id}],
                'table_id=(SELECT MAX(table_id)
                           FROM [${google_bigquery_dataset.datastore_export_dataset.id}.__TABLES__]
                           WHERE LEFT(table_id, 7) = "Backup_")' ) AS last_table
            )
        EOF
    use_legacy_sql = true
  }

  depends_on = [
    google_bigquery_dataset.datastore_export_views_legacy_view]
}

resource "google_bigquery_table" "all_backups_view" {
  project = local.datastore_export_project
  dataset_id = var.datastore_export_views_dataset
  table_id = "all_backups"

  view {
    query = <<EOF
            SELECT
              t.project_id AS source_project_id,
              t.dataset_id AS source_dataset_id,
              t.table_id AS source_table_id,
              t.partition_id AS source_partition_id,
              t.last_checked AS source_table_last_checked,
              t.id AS table_entity_id,
              b.created AS backup_created,
              b.deleted AS backup_deleted,
              b.last_modified as backup_last_modified,
              b.numBytes as backup_num_bytes,
              b.table_id as backup_table_id,
              b.dataset_id as backup_dataset_id,
              b.key AS backupBqKey
            FROM [${google_bigquery_table.last_backup_view.id}] AS b
            JOIN [${google_bigquery_table.last_table_view.id}] AS t
            ON b.parent_id = t.id
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "last_available_backup_for_every_table_entity_view" {
  project = local.datastore_export_project
  dataset_id = var.datastore_export_views_dataset
  table_id = "last_available_backup_for_every_table_entity"

  view {
    query = <<EOF
          SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY table_entity_id order by backup_created DESC) as rownum
            FROM [${google_bigquery_table.all_backups_view.id}]
            WHERE backup_deleted IS NULL
          )
          WHERE rownum=1
        EOF
    use_legacy_sql = true
  }
}