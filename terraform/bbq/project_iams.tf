resource "google_project_iam_member" "project_indexadmin_iam" {
  project = var.bbq_project
  role = "roles/datastore.importExportAdmin"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_datastoreuser_iam" {
  project = var.bbq_project
  role = "roles/datastore.user"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_bigquerydataeditor_iam" {
  project = var.bbq_project
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_storageadmin_iam" {
  project = var.bbq_project
  role = "roles/storage.admin"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_bigqueryjobuser_iam" {
  project = var.bbq_project
  role = "roles/bigquery.jobUser"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_project_bigquerydataeditor_iam" {
  project = var.bbq_metadata_project
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_project_bigqueryjobuser_iam" {
  project = var.bbq_metadata_project
  role = "roles/bigquery.jobUser"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_project_storageobjectviewer_iam" {
  project = var.bbq_metadata_project
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

