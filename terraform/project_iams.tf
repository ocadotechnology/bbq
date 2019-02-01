// Uncomment after fixing https://github.com/terraform-providers/terraform-provider-google/issues/2007
//resource "google_project_iam_binding" "project_editor_iams" {
//  project = "${var.bbq_project}"
//  role    = "roles/editor"
//
//  members = [
//  ]
//}

resource "google_project_iam_member" "project_indexadmin_iam" {
  project = "${var.bbq_project}"
  role    = "roles/datastore.indexAdmin"
  member  = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_datastoreuser_iam" {
  project = "${var.bbq_project}"
  role    = "roles/datastore.user"
  member  = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_bigquerydataeditor_iam" {
  project = "${var.bbq_project}"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_storageadmin_iam" {
  project = "${var.bbq_project}"
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}

resource "google_project_iam_member" "project_bigqueryjobuser_iam" {
  project = "${var.bbq_project}"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.bbq_project}@appspot.gserviceaccount.com"
}
