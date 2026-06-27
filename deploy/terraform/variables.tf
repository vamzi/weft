###############################################################################
# Root input variables
#
# This stack provisions the security-load-bearing parts of the Weft EKS control
# plane: the cluster (with etcd Secret encryption), the OIDC provider that backs
# IRSA, an IMDSv2-hardened node launch template, the scoped gateway IRSA role,
# and per-tenant compute-cluster IRSA roles (via modules/cluster-irsa).
#
# VPC/subnets, the RDS metastore, ECR and the ALB are owned by sibling stacks;
# they are consumed here as inputs (subnet_ids, workspace_bucket) so this stack
# stays a focused, independently-reviewable security boundary.
###############################################################################

variable "cluster_name" {
  description = "Name of the EKS cluster. Used as a prefix for all IAM/KMS resource names."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS control-plane Kubernetes version."
  type        = string
  default     = "1.30"
}

variable "subnet_ids" {
  description = "Private subnet IDs for the EKS control plane ENIs and the worker node group."
  type        = list(string)
}

variable "cluster_security_group_ids" {
  description = "Additional security groups to attach to the EKS control-plane ENIs. Optional."
  type        = list(string)
  default     = []
}

variable "endpoint_public_access" {
  description = "Whether the EKS API server is reachable from the public internet. Keep false for the hardened posture; the gateway reaches the API over the private endpoint."
  type        = bool
  default     = false
}

variable "public_access_cidrs" {
  description = "If endpoint_public_access is true, the CIDR allowlist for the public API endpoint. Never use 0.0.0.0/0 in production."
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------------------
# Worker node group
# ---------------------------------------------------------------------------

variable "create_default_node_group" {
  description = "Create a managed node group bound to the hardened launch template. Set false if Karpenter (or another provisioner) owns capacity — it must still apply the same metadata_options (see SECURITY.md)."
  type        = bool
  default     = true
}

variable "node_instance_types" {
  description = "Instance types for the default managed node group."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_desired_size" {
  description = "Desired worker count for the default node group."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum worker count for the default node group."
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum worker count for the default node group."
  type        = number
  default     = 10
}

variable "node_disk_size_gb" {
  description = "EBS root volume size (GiB) for workers. Encrypted gp3."
  type        = number
  default     = 50
}

# ---------------------------------------------------------------------------
# Gateway control-plane IRSA
# ---------------------------------------------------------------------------

variable "gateway_namespace" {
  description = "Kubernetes namespace of the control-plane gateway ServiceAccount."
  type        = string
  default     = "weft-system"
}

variable "gateway_service_account" {
  description = "Name of the gateway ServiceAccount the IRSA role is pinned to."
  type        = string
  default     = "weft-gateway"
}

variable "gateway_allow_ec2_describe" {
  description = "Grant the gateway role the single legacy action ec2:DescribeInstances. Leave false: the gateway maps cluster->role purely in-cluster (it only annotates pod ServiceAccounts) and needs NO AWS permissions by default. See SECURITY.md control #2."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Per-tenant compute clusters (IRSA via modules/cluster-irsa)
# ---------------------------------------------------------------------------

variable "workspace_bucket" {
  description = "Name of the shared S3 workspace bucket. Each per-cluster IRSA role is scoped to exactly ONE prefix within it."
  type        = string
}

variable "irsa_permissions_boundary_arn" {
  description = "Optional IAM permissions boundary applied to every per-cluster IRSA role (defense in depth: caps what a role could ever do even if its inline policy were widened by mistake)."
  type        = string
  default     = ""
}

variable "clusters" {
  description = <<-EOT
    Per-tenant compute clusters to pre-provision scoped IRSA roles for, keyed by
    cluster id. The gateway materializes the pod for cluster <id> with a
    ServiceAccount annotated with the matching role ARN (see outputs). Each role
    can read ONLY its own S3 prefix and ONE Glue database.
  EOT
  type = map(object({
    namespace       = string
    service_account = string
    s3_prefix       = string               # read-only object prefix inside workspace_bucket (no leading/trailing slash)
    glue_database   = string               # the single Glue database this cluster may read metadata for
    s3_kms_key_arn  = optional(string, "") # if the data is SSE-KMS, the key to allow Decrypt on
  }))
  default = {}
}

variable "tags" {
  description = "Tags applied to all taggable resources."
  type        = map(string)
  default     = {}
}
