###############################################################################
# Weft EKS — security control plane
#
# Controls encoded here (see SECURITY.md for the full threat model):
#   #3  IMDSv2-required node launch template, hop limit 1   -> aws_launch_template.node
#   #4  etcd Secret encryption at rest with a CMK           -> aws_kms_key.eks_secrets + encryption_config
#   #5  EKS OIDC provider that backs IRSA                   -> aws_iam_openid_connect_provider.eks
#   #2  scoped gateway IRSA role                            -> aws_iam_role.gateway
#   #1  per-tenant scoped IRSA roles (sub+aud pinned)       -> module.cluster_irsa
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  # The OIDC issuer host+path WITHOUT the scheme, e.g.
  #   oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE
  # This exact string prefixes the `:sub` / `:aud` condition keys in every
  # IRSA trust policy. replace() is idempotent if the scheme is already absent.
  oidc_provider_url = replace(aws_iam_openid_connect_provider.eks.url, "https://", "")
}

###############################################################################
# Control #4 — etcd encryption at rest
#
# Threat: a backup/snapshot of etcd, or anyone with etcd read access, sees every
# Kubernetes Secret in plaintext (DB passwords, the Anthropic API key, etc.).
# Envelope-encrypting the `secrets` resource with a customer-managed KMS key
# means Secrets are ciphertext at rest; decryption requires the CMK.
###############################################################################

data "aws_iam_policy_document" "eks_secrets_kms" {
  # Account root keeps full administration so the key can never be orphaned/locked out.
  statement {
    sid       = "AccountRootAdmin"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  # The EKS control-plane role may use the key for envelope encryption only —
  # no key administration (no kms:Put*/kms:Schedule*/kms:Disable*).
  statement {
    sid    = "AllowEksEnvelopeEncryption"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
      "kms:CreateGrant",
    ]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.eks_cluster.arn]
    }
  }
}

resource "aws_kms_key" "eks_secrets" {
  description             = "Envelope-encryption CMK for Kubernetes Secrets in etcd (${var.cluster_name})"
  deletion_window_in_days = 30
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.eks_secrets_kms.json

  tags = merge(var.tags, { Name = "${var.cluster_name}-eks-secrets" })
}

resource "aws_kms_alias" "eks_secrets" {
  name          = "alias/${var.cluster_name}-eks-secrets"
  target_key_id = aws_kms_key.eks_secrets.key_id
}

###############################################################################
# EKS control-plane IAM role
###############################################################################

data "aws_iam_policy_document" "eks_cluster_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_cluster" {
  name               = "${var.cluster_name}-eks-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_cluster_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

###############################################################################
# EKS cluster
###############################################################################

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.kubernetes_version

  vpc_config {
    subnet_ids              = var.subnet_ids
    security_group_ids      = var.cluster_security_group_ids
    endpoint_private_access = true
    endpoint_public_access  = var.endpoint_public_access
    public_access_cidrs     = var.endpoint_public_access ? var.public_access_cidrs : null
  }

  # Control #4: encrypt Kubernetes Secrets at rest in etcd with the CMK above.
  encryption_config {
    provider {
      key_arn = aws_kms_key.eks_secrets.arn
    }
    resources = ["secrets"]
  }

  # Audit + authenticator logs let us detect IRSA abuse (e.g. a pod assuming a
  # role it should not) and API access anomalies.
  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]

  tags = var.tags
}

###############################################################################
# Control #5 — EKS OIDC provider (the trust anchor for ALL IRSA roles)
#
# Every IRSA trust policy federates against this provider's ARN AND pins the
# token's `sub` and `aud`. Without this resource there is no web-identity
# federation and the cluster-irsa module has nothing to depend on.
###############################################################################

data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer

  # IRSA tokens are always minted for the STS audience. Listing it here is the
  # provider-level allowlist; the per-role trust policies additionally PIN it.
  client_id_list = ["sts.amazonaws.com"]

  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]

  tags = merge(var.tags, { Name = "${var.cluster_name}-oidc" })
}

###############################################################################
# Worker node IAM role
###############################################################################

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
  tags               = var.tags
}

# The minimum AWS-managed policies for an EKS worker. NOTE: this role is the
# prize an attacker wants — control #3 (hop limit 1) is what stops a compromised
# pod from reaching IMDS and assuming THIS role to bypass IRSA scoping.
resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

###############################################################################
# Control #3 — IMDSv2-required, hop-limit-1 node launch template
#
# Threat: a compromised pod curls the node's Instance Metadata Service
# (169.254.169.254) and reads the NODE role's credentials. The node role is far
# broader than any per-cluster IRSA role, so this is a full IRSA-bypass.
#
# Mitigation, two layers:
#   http_tokens = "required"        -> IMDSv1 (unauthenticated GET) is OFF; a
#                                      caller must do the PUT token handshake.
#   http_put_response_hop_limit = 1 -> the token PUT response TTL only survives
#                                      ONE hop. The node itself is hop 1; a
#                                      container behind the node's network
#                                      namespace is hop 2, so the packet's TTL
#                                      hits 0 before the metadata reply returns.
#                                      Pods therefore cannot complete the IMDSv2
#                                      handshake at all.
#
# Pods that legitimately need AWS creds use IRSA (projected SA token -> STS),
# never IMDS — so clamping IMDS to the node does not break workloads.
###############################################################################

resource "aws_launch_template" "node" {
  name_prefix = "${var.cluster_name}-node-"
  description = "EKS worker LT: IMDSv2 required, metadata hop limit 1, encrypted root EBS."

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 only — reject unauthenticated IMDSv1
    http_put_response_hop_limit = 1          # node-only; defeats pod -> IMDS node-credential theft
    instance_metadata_tags      = "disabled"
  }

  monitoring {
    enabled = true
  }

  # Encrypt the worker root volume; ephemeral scratch and pulled images live here.
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.node_disk_size_gb
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(var.tags, { Name = "${var.cluster_name}-node" })
  }

  # NOTE: intentionally NO image_id / iam_instance_profile / user_data here.
  # A managed node group injects the EKS-optimized AMI, the node instance
  # profile, and the bootstrap user-data; specifying them would break the
  # managed bootstrap. We only own the security-relevant knobs.
  tags = var.tags
}

resource "aws_eks_node_group" "default" {
  count = var.create_default_node_group ? 1 : 0

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-default"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids
  instance_types  = var.node_instance_types

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]

  tags = var.tags
}

###############################################################################
# Control #2 — Gateway control-plane IRSA role (scoped)
#
# The gateway provisions per-user compute pods, but it does NOT create or assume
# IAM roles to do so: the per-cluster roles are pre-provisioned by Terraform
# (module.cluster_irsa) and the gateway simply annotates each pod's
# ServiceAccount with the right role ARN — a pure Kubernetes API operation.
# Consequently the gateway needs NO standing AWS permissions by default.
#
# Its trust policy is still sub+aud pinned so the role can't be assumed by any
# other pod. The only optional grant is the single read-only ec2:DescribeInstances
# action for a legacy node-topology lookup, gated behind a flag and off by default.
###############################################################################

data "aws_iam_policy_document" "gateway_trust" {
  statement {
    sid     = "GatewayIrsaSubAudPinned"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.gateway_namespace}:${var.gateway_service_account}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "gateway" {
  name                 = "${var.cluster_name}-gateway"
  assume_role_policy   = data.aws_iam_policy_document.gateway_trust.json
  permissions_boundary = var.irsa_permissions_boundary_arn != "" ? var.irsa_permissions_boundary_arn : null
  tags                 = var.tags
}

# Legacy/optional inline policy. ec2:Describe* cannot be resource-scoped (the API
# is account-global), so it is resources=["*"], but it is strictly read-only and
# exposes no data-plane content. Created ONLY when the flag is set; otherwise the
# gateway role carries zero AWS permissions.
data "aws_iam_policy_document" "gateway" {
  statement {
    sid       = "LegacyEc2DescribeInstances"
    effect    = "Allow"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "gateway" {
  count  = var.gateway_allow_ec2_describe ? 1 : 0
  name   = "legacy-ec2-describe"
  role   = aws_iam_role.gateway.id
  policy = data.aws_iam_policy_document.gateway.json
}

###############################################################################
# Control #1 — Per-tenant compute-cluster IRSA roles (sub+aud pinned, least priv)
#
# One role per entry in var.clusters. Each is locked to exactly one pod identity
# (sub) and the STS audience (aud), and may read only its own S3 prefix and one
# Glue database. The gateway annotates the pod's ServiceAccount with the role ARN
# from the outputs.
###############################################################################

module "cluster_irsa" {
  source   = "./modules/cluster-irsa"
  for_each = var.clusters

  cluster_name = var.cluster_name
  cluster_id   = each.key

  oidc_provider_arn = aws_iam_openid_connect_provider.eks.arn
  oidc_provider_url = local.oidc_provider_url

  namespace       = each.value.namespace
  service_account = each.value.service_account

  s3_bucket          = var.workspace_bucket
  s3_prefix          = each.value.s3_prefix
  s3_kms_key_arn     = each.value.s3_kms_key_arn
  glue_database_name = each.value.glue_database

  partition      = data.aws_partition.current.partition
  aws_region     = data.aws_region.current.name
  aws_account_id = data.aws_caller_identity.current.account_id

  permissions_boundary_arn = var.irsa_permissions_boundary_arn
  tags                     = var.tags
}
