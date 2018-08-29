resource "google_bigquery_dataset" "SLO_views_for_datastudio_legacy_dataset" {
  dataset_id = "SLO_views_for_datastudio"
  project = "${var.slos_views_destination_project}"
  location = "${var.SLO_views_location}"
}

resource "google_bigquery_table" "SLI_X_days_view" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "SLO_views_for_datastudio"
  table_id = "SLI_X_days"

  view {
    query = <<EOF
          #legacySQL
          SELECT * FROM
            (select *, "3" as days from [${var.slos_views_destination_project}.${var.SLO_views_legacy}.SLI_3_days]),
            (select *, "4" as days from [${var.slos_views_destination_project}.${var.SLO_views_legacy}.SLI_4_days]),
            (select *, "5" as days from [${var.slos_views_destination_project}.${var.SLO_views_legacy}.SLI_5_days]),
            (select *, "7" as days from [${var.slos_views_destination_project}.${var.SLO_views_legacy}.SLI_7_days])
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_3_days_view", "google_bigquery_table.SLI_4_days_view", "google_bigquery_table.SLI_5_days_view", "google_bigquery_table.SLI_7_days_view"]
}

resource "google_bigquery_table" "SLI_days_by_count_view" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "SLO_views_for_datastudio"
  table_id = "SLI_days_by_count"

  view {
    query = <<EOF
            #legacySQL
            SELECT days, count(*) as count FROM [SLO_views_for_datastudio.SLI_X_days] GROUP BY days
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_X_days_view"]
}