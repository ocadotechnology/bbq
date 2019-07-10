variable "firewall_default_rule_priority" {
  //change to 2147483647 once https://github.com/terraform-providers/terraform-provider-google/issues/3074 will be fixed
  default = 2147483646
  description = "Default firewall rule priority, which is least important"
}

resource "google_app_engine_firewall_rule" "gae_firewall_rule_allow_cron" {
  project = var.bbq_project
  priority = 1000
  action = "ALLOW"
  source_range = "0.1.0.1"
  description = "Allow GAE cron requests"
}

resource "google_app_engine_firewall_rule" "gae_firewall_rule_allow_task_queue" {
  project = var.bbq_project
  priority = 1100
  action = "ALLOW"
  source_range = "0.1.0.2"
  description = "Allow GAE task queue requests"
}

resource "google_app_engine_firewall_rule" "gae_firewall_rule_deny_all" {
  project = var.bbq_project
  priority = var.firewall_default_rule_priority
  action = "DENY"
  source_range = "*"
  description = "Deny all access by default"
}