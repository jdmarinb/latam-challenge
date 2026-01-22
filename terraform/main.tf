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

# 1. Bucket de datos
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-lake"
  location      = var.region
  force_destroy = true
}

# 2. Código fuente (ZIP)
resource "google_storage_bucket_object" "source_code" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.data_lake.name
  source = "./source.zip"
}

# 3. Cloud Function Gen 2
resource "google_cloudfunctions2_function" "tweet_processor" {
  name        = "tweet-processor"
  location    = var.region
  description = "Procesa tweets (Soporta HTTPS y GCS Events)"

  build_config {
    runtime     = "python310"
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

# Permitir invocación pública (Pruebas HTTP)
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
