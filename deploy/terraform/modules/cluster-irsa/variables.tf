###############################################################################
# modules/cluster-irsa — input variables
#
# Creates ONE per-cluster (or per-tenant) IRSA role whose trust policy pins both
# the OIDC `sub` (the exact ServiceAccount) and `aud` (sts.amazonaws.com), with a
# least-privilege inline policy: read-only on ONE S3 prefix and ONE Glue database.
###############################################################################

variable "cluster_name" {
  description = "EKS cluster name, used only as a role-name prefix."
  type        = string
}

variable "cluster_id" {
  description = "Identifier of the per-tenant compute cluster this role serves (e.g. the WeftCluster name). Used in the role name and as the prefix default."
  type        = string
}

variable "role_name" {
  description = "Override the generated role name. Default: <cluster_name>-irsa-<cluster_id>."
  type        = string
  default     = ""
}

# ---------------------------------------------------------------------------
# OIDC trust anchor (control #1 — sub + aud pinning)
# ---------------------------------------------------------------------------

variable "oidc_provider_arn" {
  description = "ARN of the EKS IAM OIDC provider (aws_iam_openid_connect_provider.eks.arn)."
  type        = string
}

variable "oidc_provider_url" {
  description = "OIDC issuer host+path WITHOUT scheme (e.g. oidc.eks.us-east-1.amazonaws.com/id/ABC123). Prefixes the :sub / :aud condition keys."
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace of the pod ServiceAccount this role is pinned to."
  type        = string
}

variable "service_account" {
  description = "Name of the ServiceAccount this role is pinned to. The trust `sub` becomes system:serviceaccount:<namespace>:<service_account>."
  type        = string
}

variable "audience" {
  description = "OIDC audience to pin. EKS IRSA tokens always carry sts.amazonaws.com; do not change without reason."
  type        = string
  default     = "sts.amazonaws.com"
}

# ---------------------------------------------------------------------------
# Least-privilege data access (one S3 prefix + one Glue database)
# ---------------------------------------------------------------------------

variable "s3_bucket" {
  description = "Name of the S3 bucket holding this cluster's data."
  type        = string
}

variable "s3_prefix" {
  description = "The single object key prefix (no leading/trailing slash) this role may read. Reads are limited to <bucket>/<prefix>/*."
  type        = string

  validation {
    condition     = !startswith(var.s3_prefix, "/") && !endswith(var.s3_prefix, "/") && var.s3_prefix != ""
    error_message = "s3_prefix must be non-empty and must not start or end with '/'."
  }
}

variable "s3_kms_key_arn" {
  description = "If the S3 data is SSE-KMS encrypted, the CMK ARN to allow kms:Decrypt on. Empty = SSE-S3 / no extra grant."
  type        = string
  default     = ""
}

variable "glue_database_name" {
  description = "The single Glue (Data Catalog) database this role may read metadata for. Read is limited to this database, its tables, and the account catalog."
  type        = string
}

# ---------------------------------------------------------------------------
# ARN construction + hardening
# ---------------------------------------------------------------------------

variable "partition" {
  description = "AWS partition (aws, aws-us-gov, aws-cn). Pass data.aws_partition.current.partition."
  type        = string
  default     = "aws"
}

variable "aws_region" {
  description = "Region of the Glue catalog (for the glue:* resource ARNs)."
  type        = string
}

variable "aws_account_id" {
  description = "Account id of the Glue catalog (for the glue:* resource ARNs)."
  type        = string
}

variable "permissions_boundary_arn" {
  description = "Optional IAM permissions boundary applied to the role (defense in depth)."
  type        = string
  default     = ""
}

variable "max_session_duration" {
  description = "Max STS session duration (seconds) for assumed credentials. 3600 = 1h."
  type        = number
  default     = 3600
}

variable "tags" {
  description = "Tags applied to the role."
  type        = map(string)
  default     = {}
}
