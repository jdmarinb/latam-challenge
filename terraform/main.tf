terraform {
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type    = string
  default = "latam-challenge-485101"
}

variable "region" {
  type    = string
  default = "us-central1"
}

# --- 1. AUTOMATIZACIÓN DE APIs ---
resource "google_project_service" "apis" {
  for_each = toset([
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "eventarc.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# --- 2. PERMISOS DE INFRAESTRUCTURA (Eventarc/Storage) ---

# Obtener el número de proyecto automáticamente
data "google_project" "project" {}

# Cuenta de servicio de Storage de Google
data "google_storage_project_service_account" "gcs_account" {}

# Darle permiso a GCS para publicar en Pub/Sub (Eventarc)
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
  depends_on = [google_project_service.apis]
}

# Darle permiso al agente de Eventarc
resource "google_project_iam_member" "eventarc_service_agent" {
  project = var.project_id
  role    = "roles/eventarc.serviceAgent"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
  depends_on = [google_project_service.apis]
}

# --- 3. RECURSOS DE INFRAESTRUCTURA ---

# Bloque de importación automática
import {
  to = google_storage_bucket.data_lake
  id = "${var.project_id}-lake"
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-lake"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket_object" "source_code" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.data_lake.name
  source = "./source.zip"
}

# 4. Cloud Function Gen 2
resource "google_cloudfunctions2_function" "tweet_processor" {
  name        = "tweet-processor"
  location    = var.region
  description = "Procesa tweets (Soporta HTTPS y GCS Events)"
  depends_on  = [google_project_service.apis, google_project_iam_member.eventarc_service_agent]

  build_config {
    runtime     = "python312"
    entry_point = "entrypoint"
    source {
      storage_source {
        bucket = google_storage_bucket.data_lake.name
        object = google_storage_bucket_object.source_code.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1Gi"
    timeout_seconds    = 540
    environment_variables = {
      INPUT_FILE_PATH = "gs://${google_storage_bucket.data_lake.name}/input/farmers-protest-tweets-2021-2-4.json"
    }
    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.storage.object.v1.finalized"
    retry_policy   = "RETRY_POLICY_DO_NOT_RETRY"
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.data_lake.name
    }
  }
}

resource "google_cloud_run_service_iam_member" "public_invoker" {
  location = google_cloudfunctions2_function.tweet_processor.location
  project  = google_cloudfunctions2_function.tweet_processor.project
  service  = google_cloudfunctions2_function.tweet_processor.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

output "function_url" {
  value = google_cloudfunctions2_function.tweet_processor.url
}
