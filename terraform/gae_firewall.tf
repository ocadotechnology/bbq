resource "google_app_engine_firewall_rule" "gae_firewall_rule_allow_cron" {
  provider = "google-beta"
  project = "${var.bbq_project}"
  priority = 1000
  action = "ALLOW"
  source_range = "0.1.0.1"
  description = "Allow GAE cron requests"
}

resource "google_app_engine_firewall_rule" "gae_firewall_rule_allow_task_queue" {
  provider = "google-beta"
  project = "${var.bbq_project}"
  priority = 1100
  action = "ALLOW"
  source_range = "0.1.0.2"
  description = "Allow GAE task queue requests"
}

resource "google_app_engine_firewall_rule" "gae_firewall_rule_deny_all" {
  provider = "google-beta"
  project = "${var.bbq_project}"
  priority = 2147483646 //change to 2147483647 once https://github.com/terraform-providers/terraform-provider-google/issues/3074 will be fixed
  action = "DENY"
  source_range = "*"
  description = "Deny all access by default"
}
