variable "bucket_name" {
  description = "Base name for the S3 data lake bucket"
  type        = string
  default     = "data-pipeline-e2e-datalake"
}